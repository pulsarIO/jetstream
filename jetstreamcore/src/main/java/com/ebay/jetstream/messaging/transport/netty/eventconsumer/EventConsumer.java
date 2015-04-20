/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventconsumer;

import io.netty.channel.Channel;

import java.net.InetAddress;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.interfaces.ITransportListener;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.stats.TransportStats;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.topic.TopicDefs;
import com.ebay.jetstream.messaging.transport.netty.compression.MessageDecompressionHandler;
import com.ebay.jetstream.messaging.transport.netty.config.NettyContextConfig;
import com.ebay.jetstream.messaging.transport.netty.config.NettyTransportConfig;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.ExtendedChannelPromise;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAck;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerDiscover;
import com.ebay.jetstream.messaging.transport.netty.registry.ProducerRegistry;
import com.ebay.jetstream.util.GuidGenerator;

/**
 * @author shmurthy@ebay.com
 *  This is a surrogate for the EventConsumer. It binds to a specified tcp port and ip address corresponding to the 
 *  interface that it is expected to bind to. It is provided a list of interfaces to bind to. It in turn binds to the 
 *  IP address of each of the interfaces that is contained in the list. It listens to the EventConsumerDiscovery message
 *  and responds by publishing EventConsumerAdvertisement message. It registers a Receive session handler which is a 
 *  callback object. The session handler is passed a reference to the Mina transport listener. When a new message from 
 *  the Event Producer arrives, the session handler is invoked by the Mina layer. The session handler passes the new 
 *  Jetstream message to the registered transport listener. That get's dispatched to all registered subscribers of the 
 *  message service.
 */


@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "BC_UNCONFIRMED_CAST")
public class EventConsumer implements IMessageListener {

	static String m_hostName;

	static long m_consumerId = GuidGenerator.gen();

	static {
		try {
			m_hostName = java.net.InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {

		}
	}

	static final boolean BROADCAST = true;
	static final boolean NOBROADCAST = false;

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	private EventProducerSessionHandler m_producerSessionHandler = null;
	private final ConcurrentHashMap<JetstreamTopic, AtomicLong> m_registeredTopicsDB = new ConcurrentHashMap<JetstreamTopic,

	AtomicLong>(); // this
	private String m_context = "";
	private JetstreamTopic m_ecdvertisement = new JetstreamTopic(
			TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG);
	private JetstreamTopic m_ecdiscover = new JetstreamTopic(
			TopicDefs.JETSTREAM_EVENT_CONSUMER_DISCOVER_MSG);
	private final JetstreamTopic m_ecadvisory = new JetstreamTopic(
			TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVISORY_MSG);

	private List<Acceptor> m_acceptorList = new LinkedList<Acceptor>();
	private ITransportListener m_tl = null;
	private final AtomicBoolean m_stopSendingAviseSent = new AtomicBoolean(
			false);
	private final DispatchQueueStats m_queueStats = new DispatchQueueStats();
	private EventConsumerPeriodicAdvertiser m_advertiser;

	// stats
	private LongEWMACounter m_avgMsgsRcvdPerSec;

	private final LongCounter m_totalMsgsRcvd = new LongCounter();
	private MessageServiceProxy m_messageServiceProxy;

	private ExecutorService m_threadPoolExecutor;

	private NettyContextConfig m_contextConfig;

	private final ProducerRegistry m_producerRegistry = new ProducerRegistry();

	private NettyTransportConfig m_tke;

	public static long getConsumerId() {
		return m_consumerId;
	}

	/**
	 * 
	 */
	void advertise(boolean broadcast) {

		Iterator<Acceptor> itr = m_acceptorList.iterator();

		while (itr.hasNext()) {
			Acceptor acceptor = itr.next();

			LinkedList<JetstreamTopic> topicsToAdvertise = new LinkedList<JetstreamTopic>();

			Enumeration<JetstreamTopic> topiclist = m_registeredTopicsDB.keys();

			while (topiclist.hasMoreElements()) {
				topicsToAdvertise.add(topiclist.nextElement());
			}

			EventConsumerAdvertisement eca = new EventConsumerAdvertisement(
					acceptor.getTcpPort(), acceptor.getIpAddress(), m_context,
					topicsToAdvertise);

			eca.setPriority(JetstreamMessage.INTERNAL_MSG_PRIORITY);
			eca.setWeight(m_contextConfig.getWeight());
			eca.setCompressionEnabled(m_tke.isEnableCompression());
			eca.setKryoSerializationEnabled(true);
			eca.setBelongsToAffinityPool(true); // SRM - hack to test consistent
												// hashing
			eca.setTimeStamp(System.currentTimeMillis());
			eca.setConsumerId(m_consumerId);

			if (LOGGER.isDebugEnabled())
				LOGGER.debug( eca.toString());

			try {

				if (broadcast)
					publishToAllChannels(eca);

				m_messageServiceProxy.publish(m_ecdvertisement, eca, this);

			} catch (Exception e) {

				String message = "Failed to publish advertisement messages in EventProducer Thread for context - ";
				message += m_context;
				message += " Caught Exception ";
				message += e.getMessage();

				LOGGER.error( message);
			}
		}
	}

	/**
	 * @return the acceptorList
	 */
	public List<Acceptor> getAcceptorList() {
		return m_acceptorList;
	}

	/**
	 * @return the context
	 */
	public String getContext() {
		return m_context;
	}

	/**
	 * @return
	 */
	public TransportStats getStats() {

		TransportStats stats = new TransportStats();

		stats.setMsgsRcvdPerSec(m_avgMsgsRcvdPerSec.get());
		stats.setTotalMsgsRcvd(m_totalMsgsRcvd.get()); // uncomment for GA

		return stats;
	}

	/**
	 * @return the totalMsgsRcvd
	 */
	public long getTotalMsgsRcvd() {
		return m_totalMsgsRcvd.get();
	}

	/**
	 * @param context
	 * @param tl
	 * @param addrList
	 * @param port
	 * @param msp
	 * @throws Exception
	 */
	public void init(ContextConfig cc, ITransportListener tl,
			List<InetAddress> addrList, int port,
			TransportConfig transportConfig, MessageServiceProxy proxy)
			throws Exception {

		m_tl = tl;

		m_context = cc.getContextname();

		m_contextConfig = (NettyContextConfig) cc;

		m_messageServiceProxy = proxy;

		m_avgMsgsRcvdPerSec = new LongEWMACounter(30, MessageServiceTimer
				.sInstance().getTimer());

		m_tke = (NettyTransportConfig) transportConfig;

		if (m_tke.getAdvertisementTopic() != null) {
			m_ecdvertisement = new JetstreamTopic(m_tke.getAdvertisementTopic());
			LOGGER.info(
					"Using Topic : " + m_tke.getAdvertisementTopic()
							+ " for advertisements messages");
		}

		if (m_tke.getDiscoverTopic() != null) {
			m_ecdiscover = new JetstreamTopic(m_tke.getDiscoverTopic());
			LOGGER.info( "Using Topic : " + m_tke.getDiscoverTopic()
					+ " for discover messages");
		}

		m_producerSessionHandler = new EventProducerSessionHandler(this);

		m_threadPoolExecutor = Executors.newFixedThreadPool(m_tke
				.getConsumerThreadPoolSize(), new NameableThreadFactory(
				"Jetstream-EventConsumer"));

		if (m_tke.isEnableCompression())
			MessageDecompressionHandler.initSnappy();

		Iterator<InetAddress> itr = addrList.iterator();

		while (itr.hasNext()) {
			InetAddress addr = itr.next();

			Acceptor acceptor = new Acceptor(m_tke);
			acceptor.setIpAddress(addr.getHostAddress());
			acceptor.setTcpPort(port);
			acceptor.setEnableCompression(m_tke.isEnableCompression());
			acceptor.setTcpKeepAlive(m_tke.getTcpKeepAlive());
			if (m_tke.getTcpKeepAlive())
				acceptor.setReadIdleTimeout(0);
			else
				acceptor.setReadIdleTimeout(m_tke.getIdleTimeoutInSecs());
			acceptor.bind(m_producerSessionHandler);

			m_acceptorList.add(acceptor);

		}

		subscribeToDiscoveryEvent();

		printInfo("Initialized event consumer for context - " + m_context);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessageListener#onMessage(com.ebay.
	 * jetstream.messaging.JetstreamMessage)
	 */
	public void onMessage(JetstreamMessage m) {

		if (LOGGER.isDebugEnabled())
			LOGGER.debug( m.toString());

		if (m instanceof EventConsumerDiscover) {
			printInfo("Received EventConsumerDiscover");

			advertise(NOBROADCAST);
		}

	}

	

	/**
	 * @param msg
	 */
	void publishToAllChannels(JetstreamMessage msg) {

		msg.setTopic(m_ecadvisory);

		Enumeration<Channel> channels = m_producerRegistry.getAllProducers();

		while (channels.hasMoreElements()) {
			Channel channel = channels.nextElement();
			channel.writeAndFlush(msg, new ExtendedChannelPromise(channel));
		}
	}

	/**
	 * @param message
	 */
	private void printInfo(String message) {
		LOGGER.info( message);
	}

	
	/**
	 * @param msg
	 * @param channel
	 */
	public void receive(List<JetstreamMessage> msgs) {
		
		try {

			m_tl.postMessage(msgs, m_queueStats);
			int size = msgs.size();
			m_avgMsgsRcvdPerSec.add(size);
			m_totalMsgsRcvd.addAndGet(size);

		} catch (MessageServiceException e) {

			if (e.isBufferFull()) {
				if (!m_stopSendingAviseSent.get()) {

					String message = "Failed to insert message into upstream dispatch queue for context - ";
					message += m_context;
					message += " Caught Exception ";
					message += e.getMessage();

					LOGGER.error( message);
					// postAdvise(EventConsumerAdvisory.STOP_SENDING); // SRM
					// (April 15, 2013) - removing this - no need to send advise
					// this will introduce state management which will be hard
					// to manage
					m_stopSendingAviseSent.set(true);
				}
			}
		} catch (Throwable t) {
			String message = "Failed to insert message into upstream dispatch queue for context - ";
			message += m_context;
			message += " Unknown Exception ";
			message += t.getMessage();

			LOGGER.error( message);
		}

	}

	/**
	 * @param msg
	 * @param channel
	 */
	public void receive(JetstreamMessage msg) {
		try {

			m_tl.postMessage(msg, m_queueStats);

			m_avgMsgsRcvdPerSec.increment();

			m_totalMsgsRcvd.increment();

		} catch (MessageServiceException e) {

			if (e.isBufferFull()) {
				if (!m_stopSendingAviseSent.get()) {

					String message = "Failed to insert message into upstream dispatch queue for context - ";
					message += m_context;
					message += " Caught Exception ";
					message += e.getMessage();

					LOGGER.error( message);
					// postAdvise(EventConsumerAdvisory.STOP_SENDING); // SRM
					// (April 15, 2013) - removing this - no need to send advise
					// this will introduce state management which will be hard
					// to manage
					m_stopSendingAviseSent.set(true);
				}
			}
		} catch (Throwable t) {
			String message = "Failed to insert message into upstream dispatch queue for context - ";
			message += m_context;
			message += " Unknown Exception ";
			message += t.getMessage();

			LOGGER.error( message);
		}
	}

	void postAck(long sequenceId, Channel channel) {
		EventConsumerAck ack = new EventConsumerAck(sequenceId);
		channel.writeAndFlush(ack, new ExtendedChannelPromise(channel));

	}

	/**
	 * @param topic
	 */
	public void registerTopic(JetstreamTopic topic) {

		if (!m_registeredTopicsDB.containsKey(topic)) {
			printInfo("Registering topic - " + topic.getTopicName()
					+ " for context - " + m_context);
			m_registeredTopicsDB.put(topic, new AtomicLong(-1));

			advertise(NOBROADCAST);

			if (m_advertiser == null)
				m_advertiser = new EventConsumerPeriodicAdvertiser(this, m_tke.getAdvertiseIntervalInSecs());

		}
	}

	/**
	 * 
	 */
	public void resetStats() {

		m_avgMsgsRcvdPerSec.reset();
		m_totalMsgsRcvd.reset();

	}

	/**
	 * @param acceptorList
	 *            the acceptorList to set
	 */
	public void setAcceptorList(List<Acceptor> acceptorList) {
		m_acceptorList = acceptorList;
	}

	/**
	 * @param stats
	 */
	public void setUpstreamDispatchQueueStats(DispatchQueueStats stats) {

		m_queueStats.setHighPriorityQueueDepth(stats
				.getHighPriorityQueueDepth());
		m_queueStats.setLowPriorityQueueDepth(stats.getLowPriorityQueueDepth());
		m_queueStats.setMaxQueueDepth(stats.getMaxQueueDepth());

		// processLoad(); // SRM (April 15) - removing this for now. We will
		// drop messages and increment drop counter if we overrun queue.

	}

	/**
	 * 
	 */
	public void shutdown() {

		if (m_advertiser != null)
			m_advertiser.cancel();

		m_threadPoolExecutor.shutdown();

		m_avgMsgsRcvdPerSec.destroy();

		Enumeration<Channel> channels = m_producerRegistry.getAllProducers();

		while (channels.hasMoreElements()) {
			Channel channel = channels.nextElement();
			channel.close();
		}

		Iterator<Acceptor> itr = m_acceptorList.iterator();

		LOGGER.warn( "shutting down event consumer for context - "
				+ m_context + " total events received = " + getTotalMsgsRcvd());

		while (itr.hasNext())
			itr.next().unbind();

	}

	/**
	 * 
	 */
	void subscribeToDiscoveryEvent() {
		try {
			m_messageServiceProxy.subscribe(m_ecdiscover, this, this);
		} catch (Exception e) {

			String message = "Failed to subscribe to ECDiscover messages in EventProducer Thread for context - ";
			message += m_context;
			message += " Caught Exception ";
			message += e.getMessage();

			LOGGER.error( message);

			return;
		}
	}

	/**
	 * @param topic
	 */
	public void unregisterTopic(JetstreamTopic topic) {

		if (m_registeredTopicsDB.containsKey(topic)) {
			m_registeredTopicsDB.remove(topic);

			advertise(NOBROADCAST);

		}

	}

	/**
	 * @param topic
	 */
	public void resume(JetstreamTopic topic) {

		if (!m_registeredTopicsDB.containsKey(topic)) {

			LOGGER.warn(
					"Resuming topic - " + topic.getTopicName()
							+ " for context - " + m_context);

			m_registeredTopicsDB.put(topic, new AtomicLong(-1));

			advertise(BROADCAST);

		}
	}

	/**
	 * @param topic
	 */
	public void pause(JetstreamTopic topic) {

		if (m_registeredTopicsDB.containsKey(topic)) {

			LOGGER.warn( "Pausing topic - " + topic.getTopicName()
					+ " for context - " + m_context);

			m_registeredTopicsDB.remove(topic);

			advertise(BROADCAST);

		}
	}

	/**
	 * @param channel
	 */
	public void registerChannel(io.netty.channel.Channel channel) {
		m_producerRegistry.add(channel);
	}

	/**
	 * @param channel
	 */
	public void unregisterChannel(Channel channel) {

		m_producerRegistry.remove(channel);

	}

}
