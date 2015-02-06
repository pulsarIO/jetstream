/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.MessageServiceProperties;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.interfaces.ITransportProvider;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.stats.MessageServiceStats;
import com.ebay.jetstream.messaging.stats.MessageServiceStatsController;
import com.ebay.jetstream.messaging.stats.StatsHarvestTimer;
import com.ebay.jetstream.messaging.stats.TransportStats;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.topic.TopicInfo;
import com.ebay.jetstream.util.RequestQueueProcessor;

/**
 * @author shmurthy
 * 
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public class MessageService implements ShutDownable {

	private static MessageService s_theInstance = new MessageService();

	// the following data structure is used to maintain the sequence Ids
	// corresponding to each of the topics broadcast from a publisher process.

	private static final Logger LOGGER = LoggerFactory
			.getLogger("com.ebay.jetstream.messaging");

	/**
	 * @return
	 */
	public static MessageService getInstance() {
		return s_theInstance;
	}

	// the following data structure holds data about publishers whose
	// messages are received by the receiver. The publishers
	// last sent sequenceID is stored in the publisherInfo.
	// the key is made up of guid and ip address of sender.

	private final ConcurrentHashMap<String, DispatcherInfo> m_dispatcherlist;

	private final ConcurrentHashMap<String, TopicInfo> m_publishedTopiclist;
	private final ConcurrentHashMap<String, ConcurrentLinkedQueue<ListenerInfo>> m_listenerTable;
	private byte[] m_msgOrigAddr;
	private final AtomicBoolean m_initialized = new AtomicBoolean(false);
	RequestQueueProcessor m_msgProcessor;
	RequestQueueProcessor m_internalMsgProcessor;
	private final AtomicLong m_msgRcvCounter = new AtomicLong(0);

	private final Object m_lock = new Object();

	private final DispatchQueueStats m_queueStats = new DispatchQueueStats();
	private DispatchQueueMonitor m_dqm;
	private volatile MessageServiceProperties m_messageServiceProperties;

	// stats

	private final LongCounter m_totalMsgsSent = new LongCounter();
	private final AtomicLong m_msgsRcvdPerSec = new AtomicLong(0);
	private final LongCounter m_totalMsgsRcvd = new LongCounter();
	private final LongCounter m_totalMsgsDropped = new LongCounter();
	private final LongCounter m_totalMsgsDroppedByNoContext = new LongCounter();
	private final LongEWMACounter m_avgMsgsSentPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());
	private final LongEWMACounter m_avgMsgsRcvdPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());

	private StatsHarvestTimer m_statsHarvestTimer;

	private MessageServiceProxy m_proxy;

	private AtomicBoolean m_paused = new AtomicBoolean(
			false);

	private enum UpstreamQueueState {
		FULL, EMPTY
	};

	private int m_twentyPercentCapacity;

	public boolean isPaused() {
		return m_paused.get();
	}

	
	/**
	 * Although this class was intended to be a singleton, the default
	 * constructor has public access mainly because it needs to be instantiated
	 * by Spring.
	 */
	public MessageService() {
		s_theInstance = this; // this is done mainly so that Spring can load
								// message service - generally a bad practice
		m_listenerTable = new ConcurrentHashMap<String, ConcurrentLinkedQueue<ListenerInfo>>();
		m_dispatcherlist = new ConcurrentHashMap<String, DispatcherInfo>();
		m_publishedTopiclist = new ConcurrentHashMap<String, TopicInfo>();

	}

	/**
	 * @param topic
	 * @param tml
	 */
	void addSubscriber(
			JetstreamTopic topic,
			com.ebay.jetstream.messaging.interfaces.IMessageListener tml) {

		if (!m_listenerTable.containsKey(topic.getRootContext())) {

			ConcurrentLinkedQueue<ListenerInfo> llist = new ConcurrentLinkedQueue<ListenerInfo>();

			ListenerInfo li = new ListenerInfo();

			li.m_listener = tml;
			li.m_topic = topic;

			llist.add(li);

			m_listenerTable.put(topic.getRootContext(), llist);

		} else {

			ConcurrentLinkedQueue<ListenerInfo> llist = m_listenerTable
					.get(topic.getRootContext());
			if (llist == null)
				return;
			ListenerInfo li = new ListenerInfo();

			li.m_listener = tml;
			li.m_topic = topic;

			if (!llist.contains(li))
				llist.add(li);
			else
				li = null;
		}
	}

	/**
	 * @param context
	 * @throws Exception
	 */
	private void createDispatcherRegisterWithAllTransports(JetstreamTopic topic)
			throws Exception {
		Enumeration<DispatcherInfo> dispatcherlist = m_dispatcherlist
				.elements();

		while (dispatcherlist.hasMoreElements()) {
			DispatcherInfo dispinfo = dispatcherlist.nextElement();

			if (!dispinfo.isDispatcherCreated()) {

				try {
					dispinfo.getTransport().registerListener(m_proxy);
					dispinfo.getTransport().registerTopic(topic);
				} catch (Exception e) {

					String message = "Failed to register transport listener - ";
					message += e.getMessage();

					LOGGER.error( message);

					throw e;
				}

				dispinfo.setDispatcherCreated(true);
			}
		}
	}

	/**
	 * @param context
	 * @throws Exception
	 */
	void createDispatcherRegisterWithTransport(JetstreamTopic topic)
			throws Exception {

		String context = topic.getRootContext();

		if (context.equals("/")) {
			createDispatcherRegisterWithAllTransports(topic);
			return;
		}

		DispatcherInfo dispinfo = m_dispatcherlist.get(context);

		if (dispinfo == null) {

			String message = "Did not find any transport from config for context - ";
			message += context;

			LOGGER.error( message);

			throw new MessageServiceException(
					MessageServiceException.TRANSPORT_ERROR, message);

		}

		if (!dispinfo.isDispatcherCreated()) {

			try {
				dispinfo.getTransport().registerListener(m_proxy);
				dispinfo.getTransport().registerTopic(topic);

			} catch (Exception e) {

				if (LOGGER.isWarnEnabled()) {
					String message = "Failed to register listner - ";
					message += e.getMessage();

					LOGGER.warn( message);
				}

				throw e;
			}

			dispinfo.setDispatcherCreated(true);
		} else
			dispinfo.getTransport().registerTopic(topic);

	}

	/**
	 * @param tm
	 * @return
	 */
	boolean dispatch(JetstreamMessage tm) {

		// first disptach for all subscibers who have registered to listen to
		// all messages

		dispatchMessageForContext("/", tm);

		// next disptach to subscriber who have registered interest
		// in a specific context only

		return dispatchMessageForContext(tm.getTopic().getRootContext(), tm);

	}

	/**
	 * @param topic
	 * @param tm
	 * @throws Exception
	 */
	void dispatchDownStream(JetstreamTopic topic, JetstreamMessage tm)
			throws Exception {

		if (tm != null && topic != null) {
			tm.setTopic(topic);
			tm.setMsgOrigination(m_msgOrigAddr);
			TopicInfo ti = getTopicInfo(topic.getTopicName());
			tm.setSequenceId(ti.incSeqId());
			tm.setGuid(ti.m_id);
		} else
			throw new NullPointerException("Topic or jetstreamMessage is null");

		try {

			DispatcherInfo dispinfo = m_dispatcherlist.get(topic
					.getRootContext());

			if (dispinfo == null) {
				m_totalMsgsDroppedByNoContext.increment();
				// we did not find the context something is seriously wrong.
				// report an error and return

				if (LOGGER.isDebugEnabled()) {
					String message = "Did not find any transport from config for context - ";
					message += topic.getRootContext();
					message += " - Dropping message!!";
					LOGGER.debug( message);
				}

				return;

			}

			dispinfo.getTransport().send(tm);

			m_avgMsgsSentPerSec.increment();
			m_totalMsgsSent.increment();

		} catch (Throwable e) {

			String message = "Transport Exception - ";
			message += e.getMessage();
			message += Arrays.toString(e.getStackTrace());

			if (LOGGER.isDebugEnabled()) {

				LOGGER.debug( e.getMessage());
			}

			throw new MessageServiceException(
					MessageServiceException.TRANSPORT_ERROR, message);
		}
	}

	/**
	 * @param rootContext
	 * @param tm
	 * @return
	 */
	boolean dispatchMessageForContext(String rootContext, JetstreamMessage tm) {

		ConcurrentLinkedQueue<ListenerInfo> llist = null;

		Iterator<ListenerInfo> iter = null;

		if (m_listenerTable.containsKey(rootContext)) {

			llist = m_listenerTable.get(rootContext);

			if (llist == null) {

				return false;
			}

			try {
				if (llist.size() > 0) {

					iter = llist.iterator();

					while (iter.hasNext()) {

						ListenerInfo li = iter.next();

						if (tm.getTopic().matches(li.m_topic)) {
							li.m_listener.onMessage(tm);
						}

					}

				}
			} catch (IndexOutOfBoundsException ibe) {
				return false;
			} catch (Throwable e) {
				String message = "Exception while dispatching message for topic - "
						+ tm.getTopic().getTopicName();
				message += " - Exception - ";
				message += e.getMessage();

				LOGGER.error( message);
				LOGGER.error( e.toString());

			}
		}
		return false;
	}

	/**
	 * find contexts that were previuosly provisioned but removed as part of the
	 * config change
	 * 
	 * @param msp
	 * @return
	 */
	private List<String> findDeletedContexts(MessageServiceProperties msp) {

		LinkedList<String> deletedContexts = new LinkedList<String>();

		Enumeration<String> contextlist = m_dispatcherlist.keys();

		LinkedList<String> newContexts = new LinkedList<String>();

		Iterator<TransportConfig> itr = msp.getTransports().iterator();

		while (itr.hasNext()) {

			TransportConfig tc = itr.next();

			Iterator<ContextConfig> contextItr = tc.getContextList().iterator();

			while (contextItr.hasNext()) {

				ContextConfig cc = contextItr.next();

				newContexts.add(cc.getContextname());

				if (m_dispatcherlist.containsKey(cc.getContextname())) {

					DispatcherInfo dispinfo = m_dispatcherlist.get(cc
							.getContextname());

					if (dispinfo != null) {
						if (!dispinfo.getTransport().getTransportConfig()
								.equals(tc)) // this checks for change to all
												// properties other than context
												// list
							deletedContexts.add(cc.getContextname());
						else if (!dispinfo.getTransport().getContextConfig()
								.equals(cc)) // this checks if there is a
												// specific change to context
												// list
							deletedContexts.add(cc.getContextname());
					}
				}

			}

		}

		// Now we will see if a context has been removed from the original list

		while (contextlist.hasMoreElements()) {

			String context = contextlist.nextElement();

			if (!newContexts.contains(context))
				deletedContexts.add(context);

		}

		return deletedContexts;
	}

	/**
	 * @return
	 */
	DispatchQueueStats getDispatchQueueStats() {

		DispatchQueueStats stats = new DispatchQueueStats();

		stats.setHighPriorityQueueDepth((int) m_msgProcessor
				.getPendingRequests());
		stats.setLowPriorityQueueDepth((int) m_msgProcessor
				.getPendingRequests());
		stats.setMaxQueueDepth(m_msgProcessor.getMaxQueueSz());

		return stats;
	}

	/**
	 * @return
	 */
	public MessageServiceProperties getMessageServiceProperties() {
		return m_messageServiceProperties;
	}

	/**
	 * @return
	 */
	public MessageServiceStats getStats() {

		MessageServiceStats stats = new MessageServiceStats();

		stats.setMsgsSentPerSec(m_avgMsgsSentPerSec.get());
		stats.setTotalMsgsSent(m_totalMsgsSent.get());
		stats.setMsgsRcvdPerSec(m_avgMsgsRcvdPerSec.get());
		stats.setTotalMsgsRcvd(m_totalMsgsRcvd.get());
		stats.setTotalMsgsLostByNoContext(m_totalMsgsDroppedByNoContext.get());

		stats.setHighPriorityQueueDepth((int) m_msgProcessor
				.getPendingRequests());
		stats.setLowPriorityQueueDepth((int) m_msgProcessor
				.getPendingRequests());

		stats.setTotalMsgsLost(m_totalMsgsDropped.get());

		stats.setPaused(isPaused());

		return stats;
	}

	/**
	 * @param topicname
	 * @return
	 */
	private TopicInfo getTopicInfo(String topicname) {
		TopicInfo ti = null;

		if (m_publishedTopiclist.containsKey(topicname)) {
			ti = m_publishedTopiclist.get(topicname);

		} else {
			synchronized (m_lock) {
				if (!m_publishedTopiclist.containsKey(topicname)) {
					ti = new TopicInfo();
					m_publishedTopiclist.put(topicname, ti);
				} else
					ti = m_publishedTopiclist.get(topicname);

			}
		}

		return ti;
	}

	/**
	 * @return
	 */
	public List<TransportStats> getTransportStats()
			throws IllegalStateException {

		if (!isInitialized()) {
			throw new IllegalStateException("MessageService Not Initialized");
		}

		LinkedList<TransportStats> tportStats = new LinkedList<TransportStats>();

		Enumeration<DispatcherInfo> dispatcherlist = m_dispatcherlist
				.elements();

		while (dispatcherlist.hasMoreElements()) {
			tportStats.add(dispatcherlist.nextElement().getTransport()
					.getStats());
		}

		return tportStats;
	}

	/**
 * 
 */
	public void harvestStats() {

		Iterator<Entry<String, DispatcherInfo>> itr = m_dispatcherlist
				.entrySet().iterator();

		while (itr.hasNext()) {
			Entry<String, DispatcherInfo> entry = itr.next();
			entry.getValue().getTransport().harvestStats();
		}
	}

	/**
	 * @param msp
	 * @throws MessageServiceException
	 * @throws Exception
	 */
	public void init(MessageServiceProperties msp)
			throws MessageServiceException, Exception {
		setMessageServiceProperties(msp);
	}

	/**
	 * @param configMap
	 * @throws Exception
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private void initializeMessageService() throws Exception,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		if (m_initialized.get())
			return;
		MessageServiceProperties messageServiceProperties = getMessageServiceProperties();
		try {
			m_msgOrigAddr = java.net.InetAddress.getLocalHost().getAddress();
		} catch (Exception e) {
			if (LOGGER.isWarnEnabled()) {
				String message = "Failed to get Local Host Address - ";
				message += e.getMessage();

				LOGGER.warn( message);
			}

			throw e;
		}

		// TODO : get capacity of queue form MessageServiceProperties.
		// for now hardcode

		m_msgProcessor = new RequestQueueProcessor(
				messageServiceProperties.getUpstreamDispatchQueueSize(),
				messageServiceProperties.getUpstreamDispatchThreadPoolSize(),
				"UpstreamMessageProcessor");
		m_twentyPercentCapacity = (int) (m_msgProcessor.getMaxQueueSz() * 0.2);
		
		m_internalMsgProcessor = new RequestQueueProcessor(50000, 1,
				"InternalMsgProcessor");
		m_dqm = new DispatchQueueMonitor();
		m_dqm.start();

		m_proxy = new MessageServiceProxy(this);

		if (installTransports(messageServiceProperties)) {
			m_initialized.set(true);

			// register with monitoring and control service
			Management.removeBeanOrFolder("MessageService/stats");
			Management.addBean("MessageService/stats",
					new MessageServiceStatsController(this));
			m_statsHarvestTimer = new StatsHarvestTimer();
			MessageServiceTimer.sInstance().schedulePeriodicTask(
					m_statsHarvestTimer, 1000); // harvest stats every second

		} else {
			m_initialized.set(false);
			throw new MessageServiceException(
					MessageServiceException.INITIALIZATION_ERROR,
					"Can not install Transports");
		}

	}

	/**
	 * @param msp
	 * @return
	 * @throws Exception
	 */
	private boolean installTransports(MessageServiceProperties msp)
			throws Exception {

		if (msp == null) {

			LOGGER.error(
					"Service Unavailable!! MessageServiceProperties not specified - can not load transports");

			return false;
		}

		Map<String, String> dnsContextMap = msp.getContextMap();

		if (dnsContextMap == null)
			return false;

		Iterator<TransportConfig> itr = msp.getTransports().iterator();

		while (itr.hasNext()) {

			TransportConfig tke = itr.next();

			List<ContextConfig> contextList = tke.getContextList();

			for (int i = 0; i < contextList.size(); i++) {

				ContextConfig cc = contextList.get(i);

				DispatcherInfo dispInfo = m_dispatcherlist.get(cc
						.getContextname());

				if (dispInfo == null)
					dispInfo = new DispatcherInfo();
				else if (dispInfo.getTransport().getContextConfig().equals(cc)) {
					dispInfo.getTransport().setMessageServiceProperties(msp);
					continue;
				} else {
					dispInfo.getTransport().shutdown();
					dispInfo.setTransport(null);
				}

				ITransportProvider jetstreamTransport = instantiateTransport(tke
						.getTransportClass());

				if (jetstreamTransport == null)
					continue;

				jetstreamTransport.setContextConfig(cc);

				// now get address from DNS for this context.
				// and then set the address and port pair.

				String value;

				if (tke.requireDNS()) {

					value = dnsContextMap.get(cc.getContextname()); // get IP
																	// address
																	// and port
																	// pair from
																	// DNS

					if (value == null) {
						String message = "Unable to find DNS entry for context - ";
						message += cc.getContextname();

						LOGGER.error( message);

						continue;

					}
				} else {
					value = cc.getHostAndPort();
				}

				StringTokenizer addressAndPort = new StringTokenizer(value, ",");

				// when we use DNS we will get address and port from DNS and
				// supply it to transport
				// for non-dns, the transport could get it from transport or
				// context config depending on transport

				jetstreamTransport.setAddr(addressAndPort.nextToken());
				jetstreamTransport.setPort(Integer.parseInt(addressAndPort
						.nextToken()));

				jetstreamTransport.setMessageServiceProperties(msp);

				dispInfo.setTransport(jetstreamTransport);

				try {
					dispInfo.getTransport().init(tke, msp.getNicUsage(),
							msp.getDnsMap(), m_proxy);
					dispInfo.getTransport().registerListener(m_proxy);

					// this transport may be installed as part of a config
					// change. The service
					// might already have been up. Let's check in the listener
					// table if we have
					// subscriptions for the context associated with this
					// transport. if we do then
					// it is time to register that

					ConcurrentLinkedQueue<ListenerInfo> listeners = m_listenerTable
							.get(cc.getContextname());

					if (listeners != null) {
						Iterator<ListenerInfo> listenerItr = listeners
								.iterator();

						while (listenerItr.hasNext()) {
							ListenerInfo linfo = listenerItr.next();
							dispInfo.getTransport()
									.registerTopic(linfo.m_topic);

						}
					}

					// we need to now handle any publishers that might have
					// called prepare to publish
					// and act as a proxy for them

					Enumeration<String> topicNames = m_publishedTopiclist
							.keys();

					while (topicNames.hasMoreElements()) {
						String topicname = topicNames.nextElement();

						JetstreamTopic topic = new JetstreamTopic(topicname);

						if (dispInfo.getTransport().getContextConfig()
								.getContextname()
								.equals(topic.getRootContext())) {
							dispInfo.getTransport().prepareToPublish(
									topic.getRootContext());
						}
					}

				} catch (Exception e) {
					LOGGER.error(
							"Failed to install Transport for context - "
									+ cc.getContextname() + " - "
									+ e.getLocalizedMessage());
					throw new Exception(
							"Failed to install Transport for context - "
									+ cc.getContextname() + " - "
									+ e.getLocalizedMessage(), e);
				}

				dispInfo.setDispatcherCreated(true); // BUG FIX (SRM) - Oct 25,
														// 2013 - was false
														// before - now changing
														// to true.

				m_dispatcherlist.put(cc.getContextname(), dispInfo);
			}
		}

		return true;

	}

	/**
	 * @param className
	 * @return
	 */

	ITransportProvider instantiateTransport(String className) {

		ITransportProvider tport = null;

		try {
			tport = (ITransportProvider) Class.forName(className).newInstance();
		} catch (InstantiationException e) {

			LOGGER.error( "Exception while instatiating class - "
					+ e.getMessage());

			return null;
		} catch (IllegalAccessException e) {

			LOGGER.error( "Exception while instatiating class - "
					+ e.getLocalizedMessage());

			return null;

		} catch (ClassNotFoundException e) {
			String message = "Exception while instatiating class - ";

			message += e.getMessage();

			LOGGER.error( message);

			return null;
		}

		return tport;
	}

	/**
	 * @return
	 */
	public boolean isInitialized() {
		return m_initialized.get();
	}

	/**
	 * 
	 * Signal to message service to stop sending on the specified topic. This
	 * method will unregister the TOPIC from the associated transport without
	 * unregistering the listener. This way no more events are delivered by the
	 * producer however whatever is in the pipeline will get delivered upstream.
	 * 
	 * @param topic
	 */
	public void pause(JetstreamTopic topic) throws Exception {

		if (topic == null)
			throw new NullPointerException(
					"one or more input arguments is NULL");

		DispatcherInfo dispinfo = m_dispatcherlist.get(topic.getRootContext());

		if (dispinfo != null && dispinfo.isDispatcherCreated()) {
			dispinfo.getTransport().pause(topic);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.TransportListener#postAdvise(com.ebay.jetstream
	 * .messaging.jetstreamMessage)
	 */
	void postAdvise(JetstreamMessage tm) {

		try {
			postMessage(tm, null);
		} catch (MessageServiceException e) {

			String message = "Exception while posting advise - ";

			message += e.getMessage();

			LOGGER.error( message);

		}
	}

	/**
   * 
   */
	void postDispatchQueueStats() {

		Enumeration<DispatcherInfo> dispatcherlist = m_dispatcherlist
				.elements();

		m_queueStats.setHighPriorityQueueDepth((int) m_msgProcessor
				.getPendingRequests());
		m_queueStats.setLowPriorityQueueDepth((int) m_msgProcessor
				.getPendingRequests());
		m_queueStats.setMaxQueueDepth(m_msgProcessor.getMaxQueueSz());

		while (dispatcherlist.hasMoreElements()) {
			DispatcherInfo dispinfo = dispatcherlist.nextElement();

			if (dispinfo.getTransport() != null)
				dispinfo.getTransport().setUpstreamDispatchQueueStats(
						m_queueStats);

		}
		
		if (m_paused.get()) {
			try {
				if (m_msgProcessor.hasAvailableCapacity(m_msgProcessor.getMaxQueueSz()))
					resumeTraffic();
			} catch (Exception e) {
				LOGGER.error( "Failed to resume traffic after pausing");
			}
		}

	}

	/**
	 * Post a batch of messages.
	 * 
	 * @param msgs
	 * @param stats
	 * @throws MessageServiceException
	 */
	void postMessage(List<JetstreamMessage> msgs, DispatchQueueStats stats)
			throws MessageServiceException {

		m_msgRcvCounter.addAndGet(msgs.size());

		if ((monitorUpstreamQueueAndPauseTraffic() == UpstreamQueueState.FULL) &&
				(m_paused.get())){
				if (!m_msgProcessor.hasAvailableCapacity(m_twentyPercentCapacity)) {
					m_totalMsgsDropped.increment();
					return;
				}
		}
			
		List<Runnable> requests = new ArrayList<Runnable>(msgs.size());

		for (int i = 0, t = msgs.size(); i < t; i++) {
			JetstreamMessage tm = msgs.get(i);
			if (tm.getTopic() == null) {
				m_totalMsgsDropped.increment();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"Topic is not present in incoming message");
				}
				continue;
			}
			MessageServiceRequest msr = new MessageServiceRequest(tm);

			msr.setPriority(tm.getPriority());
			msr.setSequenceid(tm.getSequenceId());

			if (msr.getPriority() == JetstreamMessage.INTERNAL_MSG_PRIORITY) {

				if (!m_internalMsgProcessor.processRequest(msr)) {
					m_totalMsgsDropped.increment();
					throw new MessageServiceException(
							MessageServiceException.BUFFER_FULL,
							"Dispatch Queue Full");
				}
				if (m_msgsRcvdPerSec.addAndGet(1) < 0)
					m_msgsRcvdPerSec.set(0);
				m_totalMsgsRcvd.increment();

			} else {
				requests.add(msr);
			}
		}

		if (!requests.isEmpty()) {
			int batchsize = requests.size();
			// SRM - July 9, 2014. we will have 1 queue for both high and lo
			// priority messages. This is because we don't have priority queue
			// with disruptor
			if (!m_msgProcessor.processBatch(requests)) {
				m_totalMsgsDropped.addAndGet(batchsize);
				throw new MessageServiceException(
						MessageServiceException.BUFFER_FULL,
						"High Priority Dispatch Queue Full - "
								+ " Requested capacity = " + batchsize
								+ " : available capacity = "
								+ m_msgProcessor.getAvailableCapacity());

			}
			m_avgMsgsRcvdPerSec.add(batchsize);
			m_totalMsgsRcvd.addAndGet(batchsize);
		}
		if (stats != null) {
			stats.setHighPriorityQueueDepth((int) m_msgProcessor
					.getPendingRequests());
			stats.setLowPriorityQueueDepth((int) m_msgProcessor
					.getPendingRequests());
			stats.setMaxQueueDepth((int) m_msgProcessor.getMaxQueueSz());

		}
	}

	private void pauseTraffic() {

		for (Entry<String, ConcurrentLinkedQueue<ListenerInfo>> entry : m_listenerTable
				.entrySet()) {
			String context = entry.getKey();
			ConcurrentLinkedQueue<ListenerInfo> listeners = entry.getValue();
			if (listeners != null) {
				Iterator<ListenerInfo> listenerItr = listeners.iterator();
				while (listenerItr.hasNext()) {
					ListenerInfo linfo = listenerItr.next();
					try {
						if (!linfo.m_topic.equals(new JetstreamTopic(
								linfo.m_topic.getRootContext()
										+ "/InternalStateAdvisory")))
							pause(linfo.m_topic);
					} catch (Exception e) {
						LOGGER.error( "Could NOT Pause topic : "
								+ linfo.m_topic);
					}
				}
			}
		}

	}

	private UpstreamQueueState getUpstreamQueueStatus() {
		if (m_msgProcessor.hasAvailableCapacity(m_msgProcessor.getMaxQueueSz() >> 1))
				return UpstreamQueueState.EMPTY;
		else
			    return UpstreamQueueState.FULL;
	}
	
	
	/**
	 * - checks MessageService dispatch queue depth(highwatermark/ lowwatermark)
	 * and raise pause/resume to producer if needed
	 * 
	 * @throws Exception
	 * 
	 */

	private UpstreamQueueState monitorUpstreamQueueAndPauseTraffic() {

		if (getUpstreamQueueStatus() == UpstreamQueueState.FULL) {

			if (m_paused.compareAndSet(false, true)) {

				pauseTraffic();
								
			}
			
			return UpstreamQueueState.FULL;
		}

		return UpstreamQueueState.EMPTY;

	}

	/**
	 * @param tm
	 * @param stats
	 * @throws MessageServiceException
	 * @throws Exception
	 */
	void postMessage(JetstreamMessage tm, DispatchQueueStats stats)
			throws MessageServiceException {

		if (tm.getTopic() == null) {
			m_totalMsgsDropped.increment();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"Topic is not present in incoming message");
			}
			return; // cannot forward message without topic
		}

		m_msgRcvCounter.getAndIncrement();

		if ((monitorUpstreamQueueAndPauseTraffic() == UpstreamQueueState.FULL) &&
			(m_paused.get())){
			if (!m_msgProcessor.hasAvailableCapacity(m_twentyPercentCapacity)) {
				m_totalMsgsDropped.increment();
				return;
			}
		}

		MessageServiceRequest msr = new MessageServiceRequest(tm);

		msr.setPriority(tm.getPriority());
		msr.setSequenceid(tm.getSequenceId());

		if (msr.getPriority() != JetstreamMessage.INTERNAL_MSG_PRIORITY) {

			// SRM - July 9, 2014. we will have 1 queue for both high and low
			// priority messages. This is because we don't have priority queues
			// with disruptors
			if (!m_msgProcessor.processRequest(msr)) {
				m_totalMsgsDropped.increment();
				throw new MessageServiceException(
						MessageServiceException.BUFFER_FULL,
						"High Priority Dispatch Queue Full - "
								+ " Requested capacity = " + 1L
								+ " : available capacity = "
								+ m_msgProcessor.getAvailableCapacity());
			}

			m_avgMsgsRcvdPerSec.increment();
			m_totalMsgsRcvd.increment();

		} else {
			if (!m_internalMsgProcessor.processRequest(msr)) {
				m_totalMsgsDropped.increment();
				throw new MessageServiceException(
						MessageServiceException.BUFFER_FULL,
						"Dispatch Queue Full");
			}
			if (m_msgsRcvdPerSec.addAndGet(1) < 0)
				m_msgsRcvdPerSec.set(0);
			m_totalMsgsRcvd.increment();
		}

		if (stats != null) {
			stats.setHighPriorityQueueDepth((int) m_msgProcessor
					.getPendingRequests());
			stats.setLowPriorityQueueDepth((int) m_msgProcessor
					.getPendingRequests());
			stats.setMaxQueueDepth((int) m_msgProcessor.getPendingRequests());

		}

	}

	/**
	 * @param topic
	 * @param tm
	 * @throws MessageServiceException
	 * @throws Exception
	 */
	public void publish(List<JetstreamTopic> topics, JetstreamMessage tm)
			throws MessageServiceException, Exception {

		if ((topics == null) || (tm == null))
			throw new NullPointerException("One of the input arguments is null");

		Iterator<JetstreamTopic> itr = topics.iterator();

		while (itr.hasNext()) {
			publish(itr.next(), tm);
		}
	}

	/**
	 * @param topic
	 * @param tm
	 * @throws MessageServiceException
	 * @throws Exception
	 */
	public void publish(JetstreamTopic topic, JetstreamMessage tm)
			throws MessageServiceException, Exception {

		if (!m_initialized.get()) {
			throw new MessageServiceException(
					MessageServiceException.SERVICE_UNINITIALIZED,
					"Message service not Initialized");
		}

		if (tm.getPriority() != JetstreamMessage.HI_PRIORITY
				&& tm.getPriority() != JetstreamMessage.LOW_PRIORITY)
			throw new MessageServiceException(
					MessageServiceException.UNSUPPORTED_MSG_PRIORITY,
					"Message Priority out of range - "
							+ Integer.toString(tm.getPriority()));

		dispatchDownStream(topic, tm);

	}

	/**
	 * This method is expected to be called by a publisher to do signal to
	 * underlying transports bound to the specified context to get ready to
	 * receive messages to be published. The transport in turn must perform all
	 * initialization required to publish a message.
	 * 
	 * @param Context
	 */
	public void prepareToPublish(JetstreamTopic topic) {

		if (topic == null)
			throw new NullPointerException(
					"one or more input arguments is NULL");

		DispatcherInfo dispinfo = m_dispatcherlist.get(topic.getRootContext());

		if (dispinfo != null) {
			if (dispinfo.getTransport() != null)
				dispinfo.getTransport()
						.prepareToPublish(topic.getRootContext());
		}
	}

	/**
   * 
   */
	public void resetStats() {

		m_avgMsgsSentPerSec.reset();
		m_totalMsgsSent.reset();
		m_avgMsgsRcvdPerSec.reset();
		m_totalMsgsRcvd.reset();
		m_totalMsgsDropped.reset();
		m_totalMsgsDroppedByNoContext.reset();
	}

	/**
	 * 
	 * Signal to message service to resume sending on the specified topic.
	 * Reregister the TOPIC with the associated transport with the assumption
	 * that the listener is still registered. This way the pipe will be reopened
	 * to deliver events upstream
	 * 
	 * @param topic
	 */
	public void resume(JetstreamTopic topic) {

		DispatcherInfo dispinfo = m_dispatcherlist.get(topic.getRootContext());

		if (dispinfo != null && dispinfo.isDispatcherCreated()) {
			dispinfo.getTransport().resume(topic);
		}
	}

	void resumeTraffic() throws Exception {

		if (m_paused.compareAndSet(true, false)) {

			for (Entry<String, ConcurrentLinkedQueue<ListenerInfo>> entry : m_listenerTable
					.entrySet()) {

				String context = entry.getKey();
				ConcurrentLinkedQueue<ListenerInfo> listeners = entry
						.getValue();

				if (listeners != null) {
					Iterator<ListenerInfo> listenerItr = listeners.iterator();
					while (listenerItr.hasNext()) {
						ListenerInfo linfo = listenerItr.next();
						try {
							if (!linfo.m_topic.equals(new JetstreamTopic(
									linfo.m_topic.getRootContext()
											+ "/InternalStateAdvisory")))
								resume(linfo.m_topic);
						} catch (Exception e) {
							LOGGER.error(
									"Could NOT Resume topic : " + linfo.m_topic);

						}
					}
				}
			}
		}

	}

	/**
	 * @param messageServiceProperties
	 * @throws Exception
	 */
	public void setMessageServiceProperties(
			MessageServiceProperties messageServiceProperties) throws Exception {

		if (messageServiceProperties == null)
			throw new NullPointerException("MessageServiceProperties is null");

		m_messageServiceProperties = messageServiceProperties;

		if (!isInitialized())
			initializeMessageService();
		else {

			try {
				// first shutdown deleted contexts
				shutdownDeletedContexts(messageServiceProperties);

				// add any new contexts that have been added to configuration
				installTransports(messageServiceProperties);
			} catch (Throwable t) {
				LOGGER.error(
						"failed to apply Message Service Properties - "
								+ t.getLocalizedMessage());
			}
		}

	}

	/**
	 * @throws Exception
	 */
	public void shutDown() {

		LOGGER.warn( "Message Service Shutting Down");

		if (m_statsHarvestTimer != null)
			m_statsHarvestTimer.cancel();

		m_avgMsgsRcvdPerSec.destroy();
		m_avgMsgsSentPerSec.destroy();

		if (m_msgProcessor != null)
			m_msgProcessor.shutdown();

		if (m_internalMsgProcessor != null)
			m_internalMsgProcessor.shutdown();

		if (m_dqm != null)
			m_dqm.shutdown();

		Enumeration<DispatcherInfo> dispatcherlist = m_dispatcherlist
				.elements();

		while (dispatcherlist.hasMoreElements()) {
			DispatcherInfo dispinfo = dispatcherlist.nextElement();

			try {
				dispinfo.getTransport().shutdown();
			} catch (Exception e) {
				if (LOGGER.isWarnEnabled()) {
					String message = "Error while shutting down - ";
					message += e.getMessage();

					LOGGER.warn( message);
				}

			}
		}

		m_dispatcherlist.clear();

		m_listenerTable.clear();

		MessageServiceTimer.sInstance().shutdown();

		m_initialized.set(false);
	}

	/**
	 * This method must be called when a config changed is being applied. It
	 * compares the context defined in the passed MessageServiceProperties with
	 * that in the disptacher list. If a context that is present in the
	 * dispatcher list but not present in the new config will be removed from
	 * the dispatcher list and the associated transport instance will be
	 * shutdown
	 * 
	 * @param msp
	 */
	private void shutdownDeletedContexts(MessageServiceProperties msp) {
		List<String> deletedContextList = findDeletedContexts(msp);

		if (!deletedContextList.isEmpty()) {

			Iterator<String> deletedContextItr = deletedContextList.iterator();

			while (deletedContextItr.hasNext()) {
				String context = deletedContextItr.next();
				try {
					DispatcherInfo dinfo = m_dispatcherlist.remove(context);
					;
					if (dinfo != null)
						dinfo.getTransport().shutdown();
				} catch (Exception e) {
					if (LOGGER.isErrorEnabled()) {
						String message = "Error while applying new config and shutting down transport for context - ";
						message += context;
						message += " - ";
						message += e.getMessage();

						LOGGER.warn( message);
					}
				}
			}
		}
	}

	/**
	 * @param topic
	 * @param tml
	 * @throws MessageServiceException
	 * @throws Exception
	 */
	public void subscribe(JetstreamTopic topic, IMessageListener tml)
			throws MessageServiceException, Exception {

		if ((topic == null) || (tml == null))
			throw new NullPointerException(
					"one or more input arguments is NULL");

		if (!m_initialized.get()) {
			throw new MessageServiceException(
					MessageServiceException.SERVICE_UNINITIALIZED,
					"Message service not Initialized");
		}

		addSubscriber(topic, tml);

		createDispatcherRegisterWithTransport(topic);

	}

	public List<ITransportProvider> getTransportProviders() {

		Enumeration<DispatcherInfo> dispatcherlist = m_dispatcherlist
				.elements();

		List<ITransportProvider> transports = new CopyOnWriteArrayList<ITransportProvider>();
		while (dispatcherlist.hasMoreElements()) {
			DispatcherInfo dispinfo = dispatcherlist.nextElement();
			transports.add(dispinfo.getTransport());
		}

		return transports;

	}

	/**
	 * @param topic
	 * @param tml
	 * @throws MessageServiceException
	 * @throws Exception
	 */
	public void unsubscribe(
			JetstreamTopic topic,
			com.ebay.jetstream.messaging.interfaces.IMessageListener tml)
			throws MessageServiceException, Exception {

		if ((topic == null) || (tml == null))
			throw new NullPointerException(
					"one or more input arguments is NULL");

		if (!m_initialized.get()) {
			throw new MessageServiceException(
					MessageServiceException.SERVICE_UNINITIALIZED,
					"Message service not Initialized");
		}

		if (m_listenerTable.containsKey(topic.getRootContext())) {

			ConcurrentLinkedQueue<ListenerInfo> llist = m_listenerTable
					.get(topic.getRootContext());

			if (llist == null)
				return;

			ListenerInfo li = new ListenerInfo();

			li.m_listener = tml;
			li.m_topic = topic;

			llist.remove(li);

			if (llist.isEmpty()) {

				DispatcherInfo dispinfo = m_dispatcherlist.get(topic
						.getRootContext());

				if (dispinfo == null) {

					if (LOGGER.isErrorEnabled()) {
						String message = "Did not find any transport from config for context - ";
						message += topic.getRootContext();

						LOGGER.error( message);

						return;
					}
				} else {
					if (dispinfo.isDispatcherCreated()) {

						dispinfo.getTransport().unregisterTopic(topic);

					}
				}

			}
		}
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}

}
