/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.common.NameableThreadFactory;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.messaging.DispatchQueueStats;
import com.ebay.jetstream.messaging.MessageServiceProxy;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.messaging.MessageServiceUncaughtExceptionHandler;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.interfaces.ITransportListener;
import com.ebay.jetstream.messaging.messagetype.AdvisoryMessage;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.stats.TransportStats;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.topic.TopicDefs;
import com.ebay.jetstream.messaging.transport.netty.NettyTransport;
import com.ebay.jetstream.messaging.transport.netty.autoflush.handler.NettyAutoFlushBatcher;
import com.ebay.jetstream.messaging.transport.netty.compression.MessageCompressionHandler;
import com.ebay.jetstream.messaging.transport.netty.config.NettyContextConfig;
import com.ebay.jetstream.messaging.transport.netty.config.NettyTransportConfig;
import com.ebay.jetstream.messaging.transport.netty.eventconsumer.EventConsumer;
import com.ebay.jetstream.messaging.transport.netty.eventscheduler.NoConsumerToScheduleException;
import com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAck;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerDiscover;
import com.ebay.jetstream.messaging.transport.netty.registry.EventConsumerAffinityRegistry;
import com.ebay.jetstream.messaging.transport.netty.registry.EventConsumerRegistry;
import com.ebay.jetstream.messaging.transport.netty.registry.EventTopicRegistry;
import com.ebay.jetstream.messaging.transport.netty.registry.Registry;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.lb.Selection;
import com.ebay.jetstream.messaging.transport.netty.serializer.KryoObjectEncoder;
import com.ebay.jetstream.messaging.transport.netty.serializer.NettyObjectEncoder;
import com.ebay.jetstream.messaging.transport.netty.serializer.StreamMessageDecoder;
import com.ebay.jetstream.util.Request;
import com.ebay.jetstream.util.disruptor.SingleConsumerDisruptorQueue;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * @author shmurthy@ebay.com This Represents all the publishers on a context associated with a nettytransport. It discovers
 *         consumers listening on the same context that this object is associated with. It discovers the EventCosumers by
 *         listening to the EventConsumerAdvertisement messages. It builds a registry of EventConsumers. Upon detecting
 *         an EventConsumer, it opens a persistent connection to each of the EventConsumers that it discovers. As events
 *         are sent down the stack, the message is dispacthed to each event consumer. It can apply different scheduling mechanisms 
 *         to schdeule events to consumers or load balance the event traffic among a pool of consumers. It uses a pluggable
 *         load balancing algorithm to do this. The event objects are stored in a cache till there is delivery
 *         confirmation. On failure the event object gets resent down the stack.
 */





public class EventProducer extends Thread implements IMessageListener, ChannelFutureListener
{

	final private static int MAX_WEIGHTS = 101;
	private static String m_hostName;
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private static final int HASH_SEED = 2147368987;

	static {

		try {

			m_hostName = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			LOGGER.error( "Failed to find host name" + e.getLocalizedMessage());
		}

	}

	// enable compression attribute key defn
	
	public static AttributeKey<Boolean> m_eckey = AttributeKey.valueOf("enableCompression");
	public static AttributeKey<Boolean> m_kskey = AttributeKey.valueOf("enableKryoSerializer");
	
	
	// stats


	HashFunction m_hf = Hashing.murmur3_128(HASH_SEED);
	private LongEWMACounter m_avgMsgsSentPerSec;
	private final LongCounter m_totalBytesSent = new LongCounter();
	private final LongCounter m_totalRawBytes = new LongCounter();
	private final LongCounter m_totalCompressedBytes = new LongCounter();
	private final LongCounter m_totalMsgsSent = new LongCounter();
	private final LongCounter m_totalRequestsSent = new LongCounter();
	private final LongCounter m_totalMsgsDropped = new LongCounter();
	private final LongCounter m_dropsForMissingAK = new LongCounter();
	private final LongCounter m_dropsForVQOverflow = new LongCounter();
	private final LongCounter m_dropsForNoConsumer = new LongCounter();
	private final LongCounter m_noConsumerAdvisories = new LongCounter();
	private final LongCounter m_vqOverflowAdvisories = new LongCounter();
	private final LongCounter m_missingAKAdvisories = new LongCounter();
	private final LongCounter m_otherMsgDropAdvisories = new LongCounter();
	private final AtomicLong m_QueueDepth = new AtomicLong(0);

	private NettyTransport m_nt;

	// downstream queues
	private SingleConsumerDisruptorQueue<Request> m_dataQueue = null;
	private SingleConsumerDisruptorQueue<Request> m_controlQueue = new SingleConsumerDisruptorQueue<Request>(3000);

	private int m_workQueueCapacity = 100000; // read this from config

	// deadConsumerQueue
	private LinkedBlockingQueue<EventConsumerInfo> m_deadConsumerQueue = null; // list

	// of
	// dead
	// consumer
	// hosts

	private EventConsumerSessionHandler m_ecSessionHandler = null;

	// registries
	private final EventConsumerAffinityRegistry m_affinityRegistry = new EventConsumerAffinityRegistry();
	private final EventConsumerRegistry m_eventConsumerRegistry = new EventConsumerRegistry(); // key
	// =
	// hostAndPort
	// String,
	// Value
	// =
	// EventConsumerInfo
	private final EventTopicRegistry m_eventTopicRegistry = new EventTopicRegistry(
			MAX_WEIGHTS); // key = JetstreamTopic,

	// =
	// hostname,
	// val
	// =
	// eventconsumerinfo.

	private JetstreamTopic m_eventConsumerAdvertisement = new JetstreamTopic(
			TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG);

	// val = list of
	// weighted event
	// consumers

	private final JetstreamTopic m_eventConsumerAdvisory = new JetstreamTopic(
			TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVISORY_MSG);

	// topic definitions

	private JetstreamTopic m_ecdiscover = new JetstreamTopic(
			TopicDefs.JETSTREAM_EVENT_CONSUMER_DISCOVER_MSG);

	private String m_context = "";

	private final AtomicBoolean m_initialized = new AtomicBoolean(false);

	// context associated with this producer

	private ITransportListener m_advisoryListener;

	private final AtomicBoolean m_crossedHighWaterMark = new AtomicBoolean(
			false);

	private NettyTransportConfig m_transportConfig;

	private MessageServiceProxy m_messageServiceProxy;

	private  Bootstrap m_bootstrap = new Bootstrap();
	
	EventLoopGroup m_group;

	private NettyContextConfig m_ncc;

	private AtomicBoolean m_shutdown = new AtomicBoolean(false);

	// Timer m_hwtimer;

	private Scheduler m_scheduler = new com.ebay.jetstream.messaging.transport.netty.eventscheduler.WeightedRoundRobinScheduler();

	private final Registry m_registry = new Registry();

	private AtomicBoolean m_discoverSent = new AtomicBoolean(false);

	private NettyAutoFlushBatcher m_autoFlushHandler;

	// dispatch queue stats
	private final DispatchQueueStats m_queueStats = new DispatchQueueStats();
	
	public final EventProducer m_epinstance = this;
	private EventConsumer m_ec;


	public Scheduler getScheduler() {
		return m_scheduler;
	}


	/**
	 * 
	 */
	public EventProducer() {
		super("Jetstream-EventProducer");
		// This is to protect the thread from getting killed because of an
		// uncaught exception
		setUncaughtExceptionHandler(new MessageServiceUncaughtExceptionHandler());
	}

	/**
	 * @param ecInfo
	 */
	void activateEventConsumer(EventConsumerInfo ecInfo) {

		// We will now check if advertisement is already present in eventConsumerRegistry.
		// we are adding this check in anticiaption of moving to trunking. With trunking this method will
		// get called multiple times once per connection

		if (!m_eventConsumerRegistry.hasConsumerWithThisAdvertisement(ecInfo.getAdvertisement())) {

			m_eventConsumerRegistry.add(ecInfo);

			m_scheduler.addEventConsumer(ecInfo);

			insertInToEventTopicRegistry(ecInfo);
		} 
	}

	/**
	 * @param ecinfo
	 * @throws Exception
	 */
	Channel activateEventConsumerSession(EventConsumerInfo ecinfo,
			boolean asyncConnect, int numConnections) throws Exception {



		if (asyncConnect) {

			createAsyncMultipleConnections(ecinfo, numConnections);


		} else {

			// now we create the configd number of connections

			// Start the client.
			ChannelFuture cf = m_bootstrap.connect(InetAddress.getByName(ecinfo.getAdvertisement().getHostName()),
					ecinfo.getAdvertisement().getListenTcpPort()); // (5)


			cf.awaitUninterruptibly((m_transportConfig.getConnectionTimeoutInSecs() + 1) * 1000);

			if (cf.isSuccess()) {
				ConsumerChannelContext ccc = new ConsumerChannelContext();
				ccc.setChannel(cf.channel());

				ecinfo.setChannelContext(cf.channel(), ccc);
				activateEventConsumer(ecinfo);
			}			
		}

		return null;
	}



	/**
	 * @param numConnections
	 */
	private void createAsyncMultipleConnections(EventConsumerInfo ecinfo, int numConnections) {

		for (int i=0; i < numConnections; i++) {

			ChannelFuture cf = null;
			try {
				
				cf = m_bootstrap.connect(new InetSocketAddress(
						InetAddress.getByName(ecinfo.getAdvertisement().getHostName()),
						ecinfo.getAdvertisement().getListenTcpPort()));
			} catch (UnknownHostException e) {
				LOGGER.error( "failed to connect to Host - " + ecinfo.getAdvertisement().getHostName() + " - "
						+ e.getLocalizedMessage());
			}

			final EventConsumerActivationRequest ar = new EventConsumerActivationRequest(
					this, ecinfo, m_transportConfig.getMaxNettyBackLog(), cf.channel());  // not sure if it makes sense to make this final

			if (cf != null)
				cf.addListener(ar);

		}
	}



	/**
	 * @param eca
	 * @param eci
	 * @return
	 */
	private boolean hasAdvertisementChanged(EventConsumerAdvertisement eca,
			EventConsumerInfo eci) {

		if (eci.getAdvertisement() == null)
			return true;
		if (!eci.getAdvertisement().equals(eca)) {
			return true;
		}

		return false;

	}

	/**
	 * 
	 */
	private void createChannelPipeline() {


		m_group = new NioEventLoopGroup(m_transportConfig.getNumConnectorIoProcessors(), 
		        new NameableThreadFactory("NettySender-" + m_transportConfig.getTransportName()));
		m_bootstrap = new Bootstrap();
		m_bootstrap.group(m_group)
		.channel(NioSocketChannel.class)
		.option(ChannelOption.TCP_NODELAY, true)
		.option(ChannelOption.SO_KEEPALIVE, m_transportConfig.getTcpKeepAlive())
		.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, m_transportConfig.getConnectionTimeoutInSecs() * 1000)
		.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				
			    StreamMessageDecoder decoder = new StreamMessageDecoder(ClassResolvers.cacheDisabled(null));
				
				if (m_autoFlushHandler != null) {
					ch.pipeline().addLast(
					        new EventConsumerStatisticsHandler(),
							new MessageCompressionHandler(),
							new IdleStateHandler(m_transportConfig.getIdleTimeoutInSecs(), 0, 0),
							m_autoFlushHandler,
							decoder,
							new NettyObjectEncoder(),
							new KryoObjectEncoder(),
							m_ecSessionHandler);
				}
				else {
					ch.pipeline().addLast(
					        new EventConsumerStatisticsHandler(),
							new MessageCompressionHandler(),
							decoder,
							new NettyObjectEncoder(),
							new KryoObjectEncoder(),
							m_ecSessionHandler);
				}
			}
		});
		
        if (m_transportConfig.getReceivebuffersize() > 0) {
            m_bootstrap.option(ChannelOption.SO_RCVBUF, m_transportConfig.getReceivebuffersize());
        }
        
        if (m_transportConfig.getSendbuffersize() > 0) {
            m_bootstrap.option(ChannelOption.SO_SNDBUF, m_transportConfig.getSendbuffersize());
        }
	}

	/**
	 * post a DiscoverEventConsumers message
	 */
	void discoverEventConsumers() {

		printInfo("sending discover message");

		EventConsumerDiscover ecd = new EventConsumerDiscover(m_context,
				m_hostName);

		ecd.setPriority(JetstreamMessage.INTERNAL_MSG_PRIORITY);

		try {
			m_messageServiceProxy.publish(m_ecdiscover, ecd, this);
		} catch (Exception e) {

			String message = "Failed to publish EventConsumerDiscover message - ";

			message += " Caught Exception ";
			message += e.getMessage();

			LOGGER.error( message);
		}

	}

	/**
	 * write the event object to the MINA IoSession. In the process create a
	 * DispatchId and add it to the IoSession object. The Dispatchid is used as
	 * a key to store the event object in the sent message bucket. The event
	 * object will remain in the bucket till the Mina layer calls back and
	 * informs us that either there has been an error or the event was
	 * successfully dispatched to the remote consumer.
	 * 
	 * @param session
	 * @param obj
	 */

	private void dispatch(EventConsumerInfo info, JetstreamMessage msg) {

		ConsumerChannelContext ccc = info.getNextChannelContext(); // added for trunking

		Channel channel;

		if (ccc == null) {
		    if (LOGGER.isDebugEnabled()) {
		        LOGGER.debug( "session is null for host - "
					+ info.getAdvertisement().getHostName());
		    }

			// now take out this consumer from event topic registry

			if (!m_transportConfig.isAsyncConnect()) {
				try {
					channel = activateEventConsumerSession(info,
							m_transportConfig.isAsyncConnect(), m_transportConfig.getConnectionPoolSz());

					ccc = info.getNextChannelContext();

				} catch (Exception e) {
					printSevere("Failed to connect to host - "
							+ info.getAdvertisement().getHostName());
					m_deadConsumerQueue.offer(info);

					if (m_advisoryListener != null) {
						postResendAdvisory(msg);
						m_noConsumerAdvisories.increment();
					}
					else {
						m_dropsForNoConsumer.increment();
						m_totalMsgsDropped.increment();
					}
					return;

				}
			} else {

				// if we are here we could have missed catching a channel that is disconnected.
				// let us reinsert the event in to downstream queue after decrement the TTL.

				if (msg.decTTL() > 0) {
					SendEventRequest ser = new SendEventRequest(this, msg);
					if (!m_dataQueue.offer(ser)) {
						if (m_advisoryListener != null) {
							postResendAdvisory(msg);
							m_noConsumerAdvisories.increment();
						}
						else {
							m_totalMsgsDropped.increment();
							m_dropsForNoConsumer.increment();
						}
					} 
				}
				else {
					if (m_advisoryListener != null) {
						postResendAdvisory(msg);
						m_noConsumerAdvisories.increment();
					}
					else {
						m_totalMsgsDropped.increment();
						m_dropsForNoConsumer.increment();
					}
				}
				return;
			}

		} else
			channel = ccc.getChannel();
		
		if (msg.getSequenceId() < 0) {
			msg.setSequenceId(info.getSeqid(msg.getTopic()));
		}


		String remoteHost = ((InetSocketAddress) channel.remoteAddress()).getHostName(); 
		if ((remoteHost.equals(m_hostName) || remoteHost.equals("127.0.0.1")) && 
				m_nt.isListeningToPort(((InetSocketAddress) channel.remoteAddress()).getPort())) {
			try {
				
				if (m_ec != null)
					m_ec.receive(msg);
				else
					m_messageServiceProxy.postMessage(msg, m_queueStats);
				m_avgMsgsSentPerSec.increment();
				m_totalMsgsSent.increment();
				return;

			} catch (MessageServiceException e) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug( "Failed to dispath upstream" + e.getLocalizedMessage());
				if (m_advisoryListener != null) {
					postResendAdvisory(msg);
				}
				else {
					m_totalMsgsDropped.increment();

				}

			}
		}


		if (!channel.isActive()) {
			removeSession(ccc);
			tryResendMessage(msg);
			return;
			
		}
		
		if (LOGGER.isDebugEnabled())
			LOGGER.debug( msg.toString());

		EventConsumerChannelFuture promise = new EventConsumerChannelFuture(
				channel, ccc.getVirtualQueueMonitor());

		promise.setconsumerChannelContext(ccc);

		promise.setMessage(msg); // we need to add the message to the future so
		// we can reschedule the message in case of
		// failure. Look at operationComplete()
		// to see how we handle this.

		//set compression flag based on consumer advertisement 
		if(info.getAdvertisement().isCompressionEnabled()){

			promise.setCompressionEnabled(true);
							
			Attribute<Boolean> attrVal = channel.attr(m_eckey);
			
			attrVal.set(promise.isCompressionEnabled());
			
		}

		if(info.getAdvertisement().isKryoSerializationEnabled()){
			
			Attribute<Boolean> attrVal = channel.attr(m_kskey);
			
			attrVal.set(true);
		}
		else {
			Attribute<Boolean> attrVal = channel.attr(m_kskey);
			
			attrVal.set(false);
		}
			

		// check if the socket is backing up - if it is we will add the event to over flow buffer - if that is also full we will drop the event
		// and post advice. Otherwise we will hit OOM errors very quickly as
		// events will start accumulating in netty queues.

		if (!ccc.getVirtualQueueMonitor().isQueueFull()) {

			promise.addListener(this);
			
			if (m_transportConfig.getAutoFlushSz() == 0)
				channel.writeAndFlush(msg, promise);
			else
				channel.write(msg, promise);
			
			ccc.getVirtualQueueMonitor().increment();


		}
		else {
			if (m_advisoryListener != null) {
				postResendAdvisory(msg);
				m_vqOverflowAdvisories.increment();
			}
			else {
				m_totalMsgsDropped.increment();
				m_dropsForVQOverflow.increment();
			}
		}


		if (LOGGER.isDebugEnabled())
			LOGGER.debug( "netty queue backlog = "
					+ ccc.getVirtualQueueMonitor().getQueueBackLog());

	}

	/**
	 * dispatch the marshalled object dowstream to to the mina layer to be sent
	 * across on the IOSession determined after executing the LB algorithm. We
	 * might want to queue the object so that it can be resent in case mina
	 * reports a problem.
	 * 
	 * @param obj
	 * @throws Exception
	 */

	void dispatchDownStream(JetstreamMessage msg) throws Exception {

		if (msg == null) {
			throw new Exception("Null object being passed in");
		}

		// we support 3 types of scheduling on a message
		// viz. we can schedule the message to all consumers associated with the
		// topic,
		// or we can schedule the message to one or more event consumers
		// listening on the message's topic by computing the mod for each of the
		// supplied
		// affinity keys and matching it to that advertised by the consumers
		// or we can Load balance across all advertised consumers using one of
		// the deployed scheduling
		// algorithms.

		if (m_scheduler.supportsAffinity()) {
			scheduleMsgForEventConsumerWithAffinity(msg);
		} else if (msg.broadcast())
			scheduleMsgForAllConsumers(msg);
		else
			// LB across all registered consumers
			scheduleMsgForLoadBalancedEventConsumer(msg);
	}



	/**
	 * @return the advisoryListener
	 */
	public ITransportListener getAdvisoryListener() {
		return m_advisoryListener;
	}


	/**
	 * @return the context
	 */
	public String getContext() {
		return m_ncc.getContextname();
	}

	public ContextConfig getContextConfig() {
		return m_ncc;
	}


	/**
	 * @return the m_QueueDepth
	 */
	public long getQueueDepth() {
		return m_QueueDepth.get();
	}

	/**
	 * @return TransportStats
	 */
	public TransportStats getStats() {

		if (LOGGER.isDebugEnabled())
			LOGGER.debug(
					"Received Request to harvest event producer stats");

		EventProducerStats stats = new EventProducerStats();

		HarvestEventProducerStatsRequest req = new HarvestEventProducerStatsRequest(
				this, stats);

		submitControlRequest(req);

		synchronized (stats) {
			try {
				stats.wait(10000); // wait max of 10 secs
			} catch (InterruptedException e) {
				String msg = "failed to gather complete event producer stats";
				LOGGER.warn( msg);
			}
		}

		stats.setMsgsSentPerSec(m_avgMsgsSentPerSec.get());
		stats.setTotalMsgsSent(m_totalMsgsSent.get());
		stats.setTotalRawBytes(m_totalRawBytes.get());
		stats.setTotalCompressedBytes(m_totalCompressedBytes.get());
		stats.setTotalRequestsSent(m_totalRequestsSent.get());
		stats.setTotalBytesSent(m_totalBytesSent.get());
		stats.setTotalMsgsDropped(m_totalMsgsDropped.get());
		stats.setDownStreamQueueBacklog(m_dataQueue.size());
		stats.setContextConfig(m_ncc);
		stats.setDropsForMissingAK(getDropsForMissingAK());
		stats.setDropsForNoConsumer(getDropsForNoConsumer());
		stats.setDropsForVQOverflow(getDropsForVQOverflow());
		stats.setMissingAKAdvisories(getMissingAKAdvisories());
		stats.setNoConsumerAdvisories(getNoConsumerAdvisories());
		stats.setVqOverflowAdvisories(getVqOverflowAdvisories());
		stats.setOtherMsgDropAdvisories(getOtherMsgDropAdvisories());

		return stats;
	}


	/**
	 * @return
	 */
	public long getNoConsumerAdvisories() {
		return m_noConsumerAdvisories.get();
	}


	/**
	 * @return
	 */
	public long getVqOverflowAdvisories() {
		return m_vqOverflowAdvisories.get();
	}


	/**
	 * @return
	 */
	public long getMissingAKAdvisories() {
		return m_missingAKAdvisories.get();
	}


	/**
	 * @return
	 */
	public long getOtherMsgDropAdvisories() {
		return m_otherMsgDropAdvisories.get();
	}


	/**
	 * @return the totalMsgsDropped
	 */
	public long getTotalMsgsDropped() {
		return m_totalMsgsDropped.get();
	}

	/**
	 * @return the m_totalMsgsSent
	 */
	public long getTotalMsgsSent() {
		return m_totalMsgsSent.get();
	}

	/**
	 * @param ecar
	 */
	void handleNewActiveConsumer(EventConsumerActivationRequest ecar) {

		submitControlRequest(ecar);

	}

	/**
	 * @param stats
	 */
	@SuppressWarnings("unchecked")
	@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY")
	public void harvestStats(EventProducerStats stats) {

		if (LOGGER.isDebugEnabled())
			LOGGER.debug( "Start harvesting event producer stats");

		// now create a clone of the event registry
		ByteArrayOutputStream out_bytes = new ByteArrayOutputStream();
		ObjectOutputStream out_stream;
		try {
			out_stream = new ObjectOutputStream(out_bytes);
			out_stream.writeObject(m_eventConsumerRegistry);
			out_stream.writeObject(m_affinityRegistry);
			out_stream.close();
		} catch (IOException e) {

			printInfo("Failed to serialize registry for stats "
					+ e.getMessage());

			synchronized (stats) {
				stats.notifyAll();
			}

			if (LOGGER.isErrorEnabled())
				LOGGER.error(
						"Failed to harvest event producer stats - "
								+ e.getMessage());

			return;
		}

		ByteArrayInputStream inBytes = new ByteArrayInputStream(
				out_bytes.toByteArray());
		ObjectInputStream inStream;
		EventConsumerRegistry eventconsumerRegistry = null;
		EventConsumerAffinityRegistry affinityRegistry = null;

		try {
			inStream = new ObjectInputStream(inBytes);
			eventconsumerRegistry = (EventConsumerRegistry) inStream
					.readObject(); // unmarshall to

			affinityRegistry = (EventConsumerAffinityRegistry) inStream
					.readObject();

		} catch (Exception e) {

			printInfo("Failed to serialize registry for stats "
					+ e.getMessage());
			synchronized (stats) {
				stats.notifyAll();
			}

			if (LOGGER.isErrorEnabled())
				LOGGER.error(
						"Failed to harvest event producer stats - "
								+ e.getMessage());

			return;
		}

		stats.setEventConsumerRegistry(eventconsumerRegistry);
		stats.setAffinityRegistry(affinityRegistry);

		synchronized (stats) {
			stats.notifyAll();
		}

		if (LOGGER.isDebugEnabled())
			LOGGER.debug( "Finished harvesting event producer stats");

	}

	/**
	 * @param context
	 * @param port
	 */
	@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BC_UNCONFIRMED_CAST")
	public void init(ContextConfig cc, int port,
			TransportConfig transportConfig, 
			MessageServiceProxy proxy,
			NettyTransport nt) throws Exception {

		if (m_hostName == null) throw new Exception("hostname not initialized");

		if (!m_initialized.get()) {

			m_nt = nt;

			m_ncc = (NettyContextConfig) cc;

			setupRegistry();

			m_context = cc.getContextname();

			m_avgMsgsSentPerSec = new LongEWMACounter(60, MessageServiceTimer
					.sInstance().getTimer());

			m_messageServiceProxy = proxy;

			m_transportConfig = (NettyTransportConfig) transportConfig;

			Scheduler scheduler = m_ncc.getScheduler();

			if (scheduler != null)
				m_scheduler = scheduler.clone();

			LOGGER.info( "Provisioned event scheduler is "
					+ m_scheduler.getClass().getName());

			// instantiate affinity calculator

			m_workQueueCapacity = m_transportConfig
					.getDownstreamDispatchQueueSize();

			if (m_transportConfig.getAdvertisementTopic()!= null) {
				m_eventConsumerAdvertisement = new JetstreamTopic(m_transportConfig.getAdvertisementTopic());
				LOGGER.info( "Using Topic : " + m_transportConfig.getAdvertisementTopic() + " for advertisements messages");
			}

			if (m_transportConfig.getDiscoverTopic() != null) {
				m_ecdiscover = new JetstreamTopic(m_transportConfig.getDiscoverTopic());
				LOGGER.info( "Using Topic : " + m_transportConfig.getDiscoverTopic() + " for discover messages");
			}

			// m_dataQueue = new LinkedBlockingQueue<Request>(m_workQueueCapacity);
			m_dataQueue =  new SingleConsumerDisruptorQueue<Request>(m_workQueueCapacity);

			m_deadConsumerQueue = new LinkedBlockingQueue<EventConsumerInfo>(
					200); // 200 elements

			m_ecSessionHandler = new EventConsumerSessionHandler(this);

			if (m_transportConfig.installAutoFlushHandler()) 
				m_autoFlushHandler = new NettyAutoFlushBatcher(m_transportConfig.getAutoFlushSz(), m_transportConfig.getAutoFlushTimeInterval());

			//Always attach compression handler, based on consumer handshake, decide on compress/decompress
			
			MessageCompressionHandler.initSnappy();

			createChannelPipeline();

			m_initialized.set(true);

			printInfo("Initalized event producer for context - " + m_context);
		}
	}

	@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DM_STRING_VOID_CTOR")
	private void subscribeToProtocolMessages() {
		try {
			m_messageServiceProxy.subscribe(m_eventConsumerAdvertisement,
					this, this);
			m_messageServiceProxy.subscribe(m_eventConsumerAdvisory, this,
					this);

		} catch (Exception e) {

			String message = new String();
			message = "Failed to subscribe to ECAAdvertisement and ECAAdviory messages in EventProducer Thread for context - ";
			message += m_context;
			message += " Caught Exception ";
			message += e.getMessage();

			LOGGER.error( message);
			return;
		}

	}

	private void setupRegistry() {
		m_registry.setPrimaryAffinityRegistry(m_affinityRegistry);
		m_registry.setConsumerRegistry(m_eventConsumerRegistry);
		m_registry.setTopicRegistry(m_eventTopicRegistry);

	}

	/**
	 * @param info
	 */
	private void insertInToEventTopicRegistry(EventConsumerInfo info) {

		printInfo(" Adding to Event Topic Registry");
		List<JetstreamTopic> topicList = info.getAdvertisement()
				.getInterestedTopics();

		Iterator<JetstreamTopic> itr = topicList.iterator();

		while (itr.hasNext()) {
			JetstreamTopic topic = itr.next();

			m_eventTopicRegistry.put(topic, info);

		}

	}

	/**
	 * @param ecinfo
	 */
	void markConsumerDead(EventConsumerInfo ecinfo) {
		m_deadConsumerQueue.offer(ecinfo);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessageListener#onMessage(com.ebay.
	 * jetstream.messaging.JetstreamMessage)
	 */
	public void onMessage(JetstreamMessage m) {

		if (m instanceof EventConsumerAdvertisement) {
			printInfo("Received EventConsumerAdvertisement");
			EventConsumerAdvertisementRequest ecar = new EventConsumerAdvertisementRequest(
					this, (EventConsumerAdvertisement) m);

			submitControlRequest(ecar);

		} 

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.ChannelFutureListener#operationComplete(org.jboss
	 * .netty.channel.ChannelFuture)
	 */
	@Override
	public void operationComplete(ChannelFuture future) throws Exception {

		if (future instanceof EventConsumerChannelFuture) {

			EventConsumerChannelFuture eccf = (EventConsumerChannelFuture) future;

			eccf.getVirtualQueueMonitor().decrement();

			if (!future.isSuccess()) {
				// we submit the message back if TTL has not expired.

				JetstreamMessage msg = eccf.getMessage();

				if (msg.broadcast()) {
					msg = null;
					return;
				}

				tryResendMessage(msg);
												
				Channel channel = ((EventConsumerChannelFuture) future).getConsumerChannelContext().channel();
                if (channel != null && !channel.isActive()) {
					removeSession(((EventConsumerChannelFuture) future).getConsumerChannelContext());
				}
				
			} else {
				m_avgMsgsSentPerSec.increment();

				m_totalMsgsSent.increment();
				if (eccf.getWrittenSize() > 0) {
				    m_totalBytesSent.addAndGet(eccf.getWrittenSize());
				    m_totalRequestsSent.increment();
				    
				    m_totalRawBytes.addAndGet(eccf.getRawBytes());
				    m_totalCompressedBytes.addAndGet(eccf.getCompressedBytes());
				}
			}
		}

	}

	/**
	 * Constructs an Advisory Message and posts the same after stuffing in the
	 * undelivered JetstreamMessage
	 */
	private void postResendAdvisory(JetstreamMessage message) {
		if (m_advisoryListener != null) {
			LOGGER.debug( " Resending Message ");
			AdvisoryMessage advMsg = new AdvisoryMessage();
			advMsg.setTopic(new JetstreamTopic(
					m_context+"/InternalStateAdvisory"));
			advMsg.setAdvisoryTopic(message.getTopic());
			advMsg.setAdvisoryCode(AdvisoryMessage.AdvisoryCode.RESEND_MESSAGE);
			advMsg.setUndeliveredMsg(message);
			if (m_advisoryListener != null)
				m_advisoryListener.postAdvise(advMsg);
		} else
			message = null;
	}

	/**
	 * @param message
	 */
	private void printInfo(String message) {
		LOGGER.info( message);
	}

	/**
	 * @param message
	 */
	private void printSevere(String message) {
		LOGGER.error( message);
	}

	/**
	 * 
	 */
	void processControlMessages() {

		int count =10;

		while (true) {

			try {
				if (m_controlQueue.peek() == null)
					return;

				Request req = m_controlQueue.take();
				if (req != null) {
					req.execute();
				}

				req = null;

			} catch (Throwable t) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug(
							"failed to process control message"
									+ t.getMessage());

			}

			if (--count == 0)
				break;  // we will process a max of 5 control messages and then give up control.

		}
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	void processDeadConsumers() {
		EventConsumerInfo info = null;

		try {

			while (true) {

				if (m_deadConsumerQueue.peek() == null)
					return;

				info = m_deadConsumerQueue.take();

				if (info.getConsumerChannelContexts().isEmpty()) { 

					EventConsumerAdvertisement eca = info.getAdvertisement();

					if (eca == null) {
						return;
					}

					// clean up the event consumer from the topic registry

					List<JetstreamTopic> topics = eca.getInterestedTopics();

					if (topics != null) {

						Iterator<JetstreamTopic> itr = topics.iterator();

						while (itr.hasNext()) {

							m_eventTopicRegistry.remove(itr.next(), info);

						}
					}

					// clean up event consumer registry

					if (LOGGER.isWarnEnabled())
						LOGGER.warn( "removing consumer at host - "
								+ info.getAdvertisement().getHostName()
								+ " from EventConsumerRegistry");

					// we want to remove the event consumer from the registry only after all connections have been 
					// disconnected.



					m_eventConsumerRegistry.remove(info);

					m_scheduler.removeEventConsumer(info);

					info = null;
				}


			}
		} catch (InterruptedException e) {

			String msg = "Caught exception while reaping dead consumers - ";
			msg += e.getMessage();

			LOGGER.warn( msg);

		}
	}

	/**
	 * @param advertisement
	 */
	void processEventConsumerAdvertisement(
			EventConsumerAdvertisement advertisement) {

		EventConsumerInfo ecinfo = null;

		if (advertisement.getContext().equals(m_context)) {

			printInfo("received advertisement for my context - " + m_context);

			printInfo("Advertisement - " + advertisement.toString());

			if (m_eventConsumerRegistry
					.hasConsumerWithThisAdvertisement(advertisement)) {

				ecinfo = m_eventConsumerRegistry.get(advertisement);

				if (ecinfo == null)
					return;

				if (hasAdvertisementChanged(advertisement, ecinfo)) {

					removeFromEventTopicRegistry(ecinfo);
					m_scheduler.removeEventConsumer(ecinfo);
					ecinfo.setAdvertisement(advertisement);
					insertInToEventTopicRegistry(ecinfo);
					m_scheduler.addEventConsumer(ecinfo);

				} else {
					ecinfo.getAdvertisement().setTimeStamp(
							advertisement.getTimeStamp().getTime());

					if (ecinfo.getConsumerChannelContexts().size() != m_transportConfig.getConnectionPoolSz()) {

						// if some connections have dropped off due to idle time we will startup those connections here as we have
						// traffic to dispatch.
						if (m_transportConfig.isAsyncConnect()) {

							try {
								activateEventConsumerSession(ecinfo,
										m_transportConfig.isAsyncConnect(), m_transportConfig.getConnectionPoolSz() - ecinfo.getConsumerChannelContexts().size());

							} catch (Exception e) {
								printSevere("Failed to connect to host - "
										+ ecinfo.getAdvertisement().getHostName());
							}

						}
					}

				}

			} else {

				// we are seeing the consumer for the first time
				ecinfo = new EventConsumerInfo();
				ecinfo.setAlertListener(m_transportConfig.getAlertListener());
				ecinfo.setTcfg(m_transportConfig);

				ecinfo.setAdvertisement(advertisement);

				insertInToEventTopicRegistry(ecinfo);


				// now activate event consumer here asynchronously

				if (m_transportConfig.isAsyncConnect()) {

					// for async transport we will add to event consumer registry and scheduler registry after we establish connection

					try {
						activateEventConsumerSession(ecinfo, true, m_transportConfig.getConnectionPoolSz());
					} catch (Exception e) {
						printSevere("Failed to connect to host - "
								+ ecinfo.getAdvertisement().getHostName());
						m_deadConsumerQueue.offer(ecinfo);

					}
				}
				else {

					// for sync connect we will insert in to scheduler's registry and eventconsumer registry here
					m_eventConsumerRegistry.add(ecinfo);

					m_scheduler.addEventConsumer(ecinfo); // BUG FIX (1/6/2014) - is called in activateEventConsumer()
				}
			}
		} else
			return;

	}

	

	/**
	 * rebalanceLoadAcrossECWithAffinity - this method is called when a dead
	 * consumer comes back alive and is discovered. The traffic for this
	 * consumer if directed elesewhere is now moved back to this rediscoved
	 * consumer.
	 * 
	 * @param topics
	 * @param affinityKey
	 */
	@SuppressWarnings("unchecked")
	private void rebalanceLoadAcrossECWithAffinity(List<JetstreamTopic> topics,
			Long affinityKey) {
		Iterator<JetstreamTopic> itr = topics.iterator();

		while (itr.hasNext()) {
			JetstreamTopic topic = itr.next();

			// check if topic in topic registry

			if (m_eventTopicRegistry.containsKey(topic)) {
				// get list of weighted event consumers subscribed to this topic

				ArrayList<LinkedList<Selection>> weightedeclist = m_eventTopicRegistry
						.get(topic);

				Iterator<LinkedList<Selection>> weightedeclistitr = weightedeclist
						.iterator();

				while (weightedeclistitr.hasNext()) {

					LinkedList<Selection> eclist = weightedeclistitr.next();

					Iterator<Selection> eclistitr = eclist.iterator();

					while (eclistitr.hasNext()) {

						EventConsumerInfo info = (EventConsumerInfo) eclistitr
								.next();

						if (info == null)
							continue;

						// check if the event consumer is bound to multiple keys
						if (info.containsMultipleAffinityKeyBindings()) {

							if (!info.isAffinityKeyBound(affinityKey))
								continue;

							// unbind the specified affinity key from this
							// consumer and remove the key from
							// affinity registry

							info.unbindAffinityKey(affinityKey);

							// Map<Integer, ConsumerChannelContext> contextMap = info.getConsumerChannelContexts();
							Map<Channel, ConsumerChannelContext> contextMap = info.getConsumerChannelContexts();

							Collection<ConsumerChannelContext> consumerContexts = contextMap.values();

							for (ConsumerChannelContext channelContext : consumerContexts) {

								List<Long> affinityKeyList = (List<Long>) channelContext
										.getAttribute("affinityKey");

								printInfo("In rebalance - removing key = "
										+ affinityKey.longValue());

								affinityKeyList.remove(affinityKey);

							}
						}
					}
				}
			}
		}
	}

	/**
	 * @param info
	 */
	private void removeFromEventTopicRegistry(EventConsumerInfo info) {

		EventConsumerAdvertisement advertisement = info.getAdvertisement();

		if (advertisement == null) // just to be safe
			return;

		List<JetstreamTopic> topicList = advertisement.getInterestedTopics();

		Iterator<JetstreamTopic> itr = topicList.iterator();

		while (itr.hasNext()) {

			JetstreamTopic topic = itr.next();

			m_eventTopicRegistry.remove(topic, info); // this consumer's
			// association with this
			// topic is going to be
			// removed
		}
	}

	/**
	 * @param ctx
	 */

	public void removeSession(ChannelHandlerContext ctx) {

		String host;
		int port;

		if (ctx == null)
			return;

		try {

			/* Remove for netty 4.0 as host port does not come back when channel gets disconnected - in fact no notification arrives that I know of. I am 
			 * tapping in to the future to tell me is channel is disconnected. For this host and port need to stored in channel context.
			 */

			host = ((InetSocketAddress) ctx.channel().remoteAddress())
					.getAddress().getHostAddress();
			port = ((InetSocketAddress) ctx.channel().remoteAddress())
					.getPort();



		} catch (Throwable t) {
			return;
		}

		if (host == null)
			return;

		printInfo("removeSession to host : " + host);

		String hostAndPort = host + "-" + Integer.toString(port);// this is so
		// that we
		// can
		// lookup in
		// the
		// register
		// using
		// "host-port"
		// - SRM Sep
		// 1,2012

		EventConsumerInfo info = m_eventConsumerRegistry.get(hostAndPort);

		if (info == null) {
			return;
		}

		// Lets close all open connections to this consumer
		
		Map<Channel, ConsumerChannelContext> channelMap = info.getConsumerChannelContexts();
		
		Set<Channel> openchannels = channelMap.keySet();
		
		for (Channel channel : openchannels) {
			channel.close();
			info.markChannelAsDisconnected(channel);
		}
		
		m_deadConsumerQueue.offer(info);
		

	}
	
	public EventConsumerInfo findEventConsumer(ChannelHandlerContext ctx) {
		
		String host;
		int port;

		if (ctx == null)
			return null;

		try {

			/* Remove for netty 4.0 as host port does not come back when channel gets disconnected - in fact no notification arrives that I know of. I am 
			 * tapping in to the future to tell me is channel is disconnected. For this host and port need to stored in channel context.
			 */

			host = ((InetSocketAddress) ctx.channel().remoteAddress())
					.getAddress().getHostAddress();
			port = ((InetSocketAddress) ctx.channel().remoteAddress())
					.getPort();



		} catch (Throwable t) {
			return null;
		}

		if (host == null)
			return null;

		printInfo("removeSession to host : " + host);

		String hostAndPort = host + "-" + Integer.toString(port);// this is so
		// that we
		// can
		// lookup in
		// the
		// register
		// using
		// "host-port"
		// - SRM Sep
		// 1,2012

		EventConsumerInfo info = m_eventConsumerRegistry.get(hostAndPort);

		
		return info;
		
	}

	public void resetStats() {

		m_avgMsgsSentPerSec.reset();
		m_totalMsgsSent.reset();
		m_totalRawBytes.reset();
		m_totalCompressedBytes.reset();
		m_totalRequestsSent.reset();
		m_ecSessionHandler.reset();
		m_totalMsgsDropped.reset();
		m_totalBytesSent.reset();
		m_dropsForMissingAK.reset();
		m_dropsForVQOverflow.reset();
		m_dropsForNoConsumer.reset();
		m_noConsumerAdvisories.reset();
		m_vqOverflowAdvisories.reset();
		m_missingAKAdvisories.reset();
		m_otherMsgDropAdvisories.reset();


	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#run()
	 */
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		// We will run this EventProducer in a seperate thread. All requests are
		// made through a single work
		// queue. We will process the queue and execute the job in the context
		// of
		// this queue. Thsi design is chosen to
		// reduce context switches.

		long closeInterval = m_transportConfig
				.getProactiveConnCloseIntervalMs();
		TimerTask proactiveCloseTask = null;
		if (closeInterval > 0) {
			proactiveCloseTask = new ProactiveConnCloseTask();
			MessageServiceTimer.sInstance().schedulePeriodicTask(
					proactiveCloseTask, closeInterval, closeInterval);
		}

		while (true) {



			Request req = null;


			try {

				// first reap dead consumers and then process work queue
				processDeadConsumers();

				processControlMessages();

				req = m_dataQueue.take();
				if (req != null && !req.execute()) {
					if(proactiveCloseTask != null) {
						try {
							proactiveCloseTask.cancel();
						} catch (Exception e) {
							LOGGER.error( e.getMessage(), e);
						}
					}
					return;
				} 
				req = null;

				// now check if we had crossed high water mark before and now
				// the queue has drained
				// sufficiently for us to signal upstream


				if (m_crossedHighWaterMark.get()
						&& m_dataQueue.size() <= (int) (0.001 * m_workQueueCapacity)) {

					// Queue has drained out - we crossed low water mark - we
					// will now post advisory to
					// upstream producers to resume sending

					m_crossedHighWaterMark.set(false);

					AdvisoryMessage advMsg = new AdvisoryMessage();

					advMsg.setTopic(new JetstreamTopic(
							TopicDefs.JETSTREAM_MESSAGING_INTERNAL_STATE_ADVISORY));

					advMsg.setAdvisoryTopic(new JetstreamTopic(m_context));
					advMsg.setAdvisoryCode(AdvisoryMessage.AdvisoryCode.RESUME_SENDING);

					if (m_advisoryListener != null)
						m_advisoryListener.postAdvise(advMsg);
				}

			} catch (Throwable e) {

				// we better not reach here. If we do we have a serious problem
				// requiring restart of the server

				String message = "Event Producer failed to execute task for context - ";
				message += m_context;
				message += " Caught Exception ";
				message += e.getMessage();

				LOGGER.error( message);
				req = null;
				continue;
			}


		}
	}

	/**
	 * sprayToAllConsumers - this method is called when the message is tagged
	 * for spraying to all consumers.
	 * 
	 * @param msg
	 */
	private void scheduleMsgForAllConsumers(JetstreamMessage msg) {

		if (LOGGER.isDebugEnabled())
			LOGGER.debug( "Spraying message - " + msg.toString());

		EventConsumerInfo info = null;

		ArrayList<LinkedList<Selection>> weightedList = m_eventTopicRegistry
				.get(msg.getTopic());

		if (weightedList != null) {

			Iterator<LinkedList<Selection>> weightedItr = weightedList
					.iterator();

			while (weightedItr.hasNext()) {

				LinkedList<Selection> consumerList = weightedItr.next();

				Iterator<Selection> itr = consumerList.iterator();

				while (itr.hasNext()) {

					info = (EventConsumerInfo) itr.next();

					if (info == null)
						continue;

					dispatch(info, msg);
				}

			}
		} else {

			if (m_advisoryListener != null) {
				postResendAdvisory(msg);
				m_noConsumerAdvisories.increment();
			}
			else {
				m_totalMsgsDropped.increment();
				m_dropsForNoConsumer.increment();
			}
		}
	}

	/**
	 * scheduleMsgForEventConsumerWithAffinity - retrieve the affinity key from
	 * the message and compute the hash of this key using the pool size. Match
	 * the hash to an advertised affinity key to locate a consumer. If the
	 * consumer with this affinity key is not yet discovered, then direct
	 * traffic to the primary backup. If primary backup is not available direct
	 * traffic to the secondary backup
	 * 
	 * @param msg
	 * @return
	 */

	private void scheduleMsgForEventConsumerWithAffinity(JetstreamMessage msg) {

		EventConsumerInfo info = null;

		if ((m_scheduler.supportsAffinity() && !msg.requiresAffinity())
				|| (m_scheduler.supportsAffinity() && msg.requiresAffinity() && msg
						.getAffinityKey() == null)) {
	if (LOGGER.isDebugEnabled())
				LOGGER.debug(
						"Affinity Scheduler Deployed but message did not come with affinity key  - Dropping message for topic - "
								+ msg.getTopic().getTopicName());

			if (m_advisoryListener != null) {
				postResendAdvisory(msg);
				m_missingAKAdvisories.increment();
			}
			else {
				m_totalMsgsDropped.increment();
				m_dropsForMissingAK.increment();
			}

			return;
		} 

		try {
			info = m_scheduler.scheduleNext(msg, m_registry);
		} catch (NoConsumerToScheduleException e) {

			if (LOGGER.isDebugEnabled())
				LOGGER.debug( "No consumer to schedule");

			info = null;

		}

		if (info == null) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug(
						"No consumer found with affinity - Dropping Message on Topic - "
								+ msg.getTopic().getTopicName());

			if (m_advisoryListener != null) {
				postResendAdvisory(msg);
				m_noConsumerAdvisories.increment();
			}
			else {
				m_totalMsgsDropped.increment();
				m_dropsForNoConsumer.increment();
			}
			return;
		}

		dispatch(info, msg);

	}

	/**
	 * This method will find the next event consumer to be scheduled for
	 * dispatch applying the deployed lb scheduling algorithm
	 * 
	 * @param topic
	 * @return
	 */

	private void scheduleMsgForLoadBalancedEventConsumer(JetstreamMessage msg) {

		// we need to pick one of the sessions using a LB algorithm
		EventConsumerInfo info = null;

		ArrayList<LinkedList<Selection>> selection = m_eventTopicRegistry
				.get(msg.getTopic());

		if (selection == null) {

			if (LOGGER.isDebugEnabled())
				LOGGER.debug(
						"No consumers found in registry for scheduling"); // convert
			// to
			// finest
			// logs

			if (m_advisoryListener != null) {
				postResendAdvisory(msg);
				m_noConsumerAdvisories.increment();
			}
			else {
				m_totalMsgsDropped.increment();
				m_dropsForNoConsumer.increment();
			}


			return; // we might not have yet received any advertisement
		}

		try {

			info = m_scheduler.scheduleNext(msg, m_registry);

		} catch (NoConsumerToScheduleException e) {

			if (LOGGER.isDebugEnabled())
				LOGGER.debug( "No Consumers found to schedule");
			info = null;
		}

		if (info == null) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug(
						"No consumers found in registry for scheduling"); // convert
			// to
			// finest
			// logs

			if (m_advisoryListener != null) {
				postResendAdvisory(msg);
				m_noConsumerAdvisories.increment();
			}
			else {
				m_totalMsgsDropped.increment();
				m_dropsForNoConsumer.increment();
			}

			return; // we might not have yet received any advertisement
		}

		dispatch(info, msg);
	}

	/**
	 * @param msg
	 * @throws Exception
	 */
	public void send(JetstreamMessage msg) throws Exception {

		if (m_shutdown.get()) return;

		if (!m_initialized.get()) {
			throw new Exception("TxSessionManager not initialized");
		}

		if (m_eventConsumerRegistry.isEmpty()) {

			if (!m_discoverSent.get()) {

				// we are forced to drop event as we have not discovered any
				// consumers.

				// we will send a discover message now just once

				subscribeToProtocolMessages();
				discoverEventConsumers();
				m_discoverSent.set(true);

				if (m_advisoryListener != null) {
					postResendAdvisory(msg);
					m_otherMsgDropAdvisories.increment();
				}
				else {
					m_totalMsgsDropped.increment();
					System.out.println("dropping message - as sending discover msg");
				}

				return; // no point throwing exception from here
			}

		}

		m_QueueDepth.set(m_dataQueue.size());

		// now examine queue depth to post advisories to upstream producers to
		// stop sending
		if (!m_crossedHighWaterMark.get()) {

			if (m_dataQueue.size() > (int) (0.85 * m_workQueueCapacity)) {

				// we crossed high water mark - we will post advisory

				m_crossedHighWaterMark.set(true);

				if (m_advisoryListener != null) {

					AdvisoryMessage advMsg = new AdvisoryMessage();

					advMsg.setTopic(new JetstreamTopic(
							TopicDefs.JETSTREAM_MESSAGING_INTERNAL_STATE_ADVISORY));

					advMsg.setAdvisoryTopic(new JetstreamTopic(m_context));
					advMsg.setAdvisoryCode(AdvisoryMessage.AdvisoryCode.STOP_SENDING);
					m_advisoryListener.postAdvise(advMsg);

				}

			}

			SendEventRequest ser = new SendEventRequest(this, msg);
			m_dataQueue.offer(ser);
			return;


		} 

		// the case for resume sending is handled in the run() method

		if (m_advisoryListener != null) {
			postResendAdvisory(msg);
			m_otherMsgDropAdvisories.increment();
		}
		else {
			m_totalMsgsDropped.increment();
			System.out.println("dropping message - as send queue full");

		}

	}

	/**
	 * @param advisoryListener
	 *            the advisoryListener to set
	 */
	public void setAdvisoryListener(ITransportListener advisoryListener) {
		m_advisoryListener = advisoryListener;
	}

	/**
	 * @param context
	 *            the context to set
	 */
	@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BC_UNCONFIRMED_CAST")
	public void setContextConfig(ContextConfig cc) {
		m_context = cc.getContextname();
		m_ncc = (NettyContextConfig) cc;
	}

	/**
	 * closes all open connections to event consumers
	 */

	void closeAllConnections() {

		// Close all connections before calling realeaseExternalResources on netty

		Enumeration<EventConsumerInfo> consumers = m_eventConsumerRegistry.getAllConsumers();

		while(consumers.hasMoreElements()) {
			EventConsumerInfo evinfo = consumers.nextElement();

			Map<Channel, ConsumerChannelContext> contextMap = evinfo.getConsumerChannelContexts();

			Collection<ConsumerChannelContext> consumerContexts = contextMap.values();

			for (ConsumerChannelContext context : consumerContexts) {

				context.getChannel().close();
			}

		}

		m_group.shutdownGracefully();

	}

	//close all connections and clear ConsumerChannelContexts
	void proactiveCloseConnections() {
		Enumeration<EventConsumerInfo> consumers = m_eventConsumerRegistry
				.getAllConsumers();

		while (consumers.hasMoreElements()) {
			EventConsumerInfo evinfo = consumers.nextElement();

			Map<Channel, ConsumerChannelContext> contextMap = evinfo.getConsumerChannelContexts();

			Collection<ConsumerChannelContext> consumerContexts = contextMap
					.values();

			boolean closed = false;
			for (ConsumerChannelContext context : consumerContexts) {
				Channel ch = context.getChannel();
				LOGGER.info( "Proactively close channel " + ch.toString()
						+ " for consumer " + evinfo.toString());
				ch.close();
				closed = true;
			}

			// only close one consumer, in single connection mode there should
			// be only one consumer that is connected
			if (closed) {
				break;
			}
		}
	}


	/**
	 * @throws MessageServiceException
	 * 
	 */
	public void shutdown() throws MessageServiceException {

		m_shutdown.set(true);

		if (m_autoFlushHandler != null)
			m_autoFlushHandler.shutdown();

		ShutdownRequest sr = new ShutdownRequest(this);

		m_dataQueue.offer(sr);

		m_avgMsgsSentPerSec.destroy();

		try {
			this.join(10000);
		} catch (InterruptedException e) {

			String message = "Failed to shutdown EventProducer Thread for context - ";
			message += m_context;
			message += " Caught Exception ";
			message += e.getMessage();

			LOGGER.error( message);

			throw new MessageServiceException(
					MessageServiceException.SHUTDOWN_FAILED, message);
		} 


		m_scheduler.shutdown();

		LOGGER.warn( "EventProducer for context - " + m_context + " shuting down"
				+ " total messages sent = " + getTotalMsgsSent() 
				+ " total messages dropped = " + getTotalMsgsDropped()
				);

	}

	/**
	 * @param req
	 */
	private void submitControlRequest(Request req) {
		m_controlQueue.offer(req);
		m_dataQueue.offer(new ControlMsgReadRequest());
	}


	public void prepareToPublish(String context) {

		if (context.equals(m_context)) {
			subscribeToProtocolMessages();
			discoverEventConsumers();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// swallow
			} 

		}
	}

	/**
	 * @return
	 */
	public int getWorkQueueCapacity() {
		return m_workQueueCapacity;
	}


	/**
	 * @return
	 */
	public long getDropsForMissingAK() {
		return m_dropsForMissingAK.get();
	}



	/**
	 * @return
	 */
	public long getDropsForVQOverflow() {
		return m_dropsForVQOverflow.get();
	}




	/**
	 * @return
	 */
	public long getDropsForNoConsumer() {
		return m_dropsForNoConsumer.get();
	}


	/**
	 * @param message
	 */
	public void processAck(EventConsumerAck message) {

		// impl for the future if we ever process Ack

	}

	class ProactiveConnCloseTask extends TimerTask {

		@Override
		public void run() {
			ProactiveConnectionCloseRequest request = new ProactiveConnectionCloseRequest(
					EventProducer.this);
			m_dataQueue.offer(request);			
		}

	}

	public void SetEventConsumer(EventConsumer ec) {
		m_ec = ec;
	}
	
	public void tryResendMessage(JetstreamMessage msg) {
		
		if (msg.decTTL() == 0) {
			// if message's life has expired,
			// drop it.
			// may be log a message here

								
			if (m_advisoryListener != null) {
				postResendAdvisory(msg);
				m_otherMsgDropAdvisories.increment();
			}
			else
				m_totalMsgsDropped.increment();

			return;
		}

		try {
			send(msg);
		} catch (Exception e) {

			if (LOGGER.isDebugEnabled()) {
				String message = "Failed to resend, Dropping message !! - ";
				message += " Caught Exception ";
				message += e.getMessage();
	
				LOGGER.debug( message);
	
				msg = null;
			}

			if (m_advisoryListener != null) {
				postResendAdvisory(msg);
				m_otherMsgDropAdvisories.increment();
			}
			else
				m_totalMsgsDropped.increment();

		}
	}


}