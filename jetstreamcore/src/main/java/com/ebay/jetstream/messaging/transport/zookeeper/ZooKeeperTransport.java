/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.NICUsage;
import com.ebay.jetstream.config.dns.DNSMap;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.DispatchQueueStats;
import com.ebay.jetstream.messaging.MessageServiceProxy;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.MessageServiceProperties;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.ITransportListener;
import com.ebay.jetstream.messaging.interfaces.ITransportProvider;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.stats.TransportStats;
import com.ebay.jetstream.messaging.stats.TransportStatsController;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.topic.TopicDefs;
import com.ebay.jetstream.messaging.transport.netty.config.NettyTransportConfig;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerDiscover;
import com.ebay.jetstream.util.AbortRequest;
import com.ebay.jetstream.util.Request;
import com.ebay.jetstream.xmlser.Hidden;
import com.google.common.net.InetAddresses;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;

/**
 * 
 * Transport implements/acts as a multicast service with underlying zookeeper
 * group membership service
 * 
 * @author rmuthupandian
 * 
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = {
		"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", "NN_NAKED_NOTIFY" })
public class ZooKeeperTransport implements ITransportProvider, Watcher,
		BackgroundCallback, ConnectionStateListener, Runnable {

	private final static Logger LOGGER = LoggerFactory.getLogger(ZooKeeperTransport.class);

	private ITransportListener m_transportListener;
	private MessageServiceProxy m_serviceProxy;
	private MessageServiceProperties m_props;
	private final DispatchQueueStats m_queueStats;
	private GroupMembership m_group;
	private final LongEWMACounter m_msgsRcvdPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());;
	private final LongEWMACounter m_msgsSentPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());;
	private final LongCounter m_totalMsgsRcvd = new LongCounter();
	private final LongCounter m_totalMsgsSent = new LongCounter();
	private final AtomicBoolean m_initialized;
	private final AtomicBoolean m_shutdown;
	private String m_addr;
	private String m_context;
	private int m_cxnWaitInMillis;
	private int m_retrycount;
	private int m_retrywaitTimeInMillis;
	private int m_sessionTimeoutInMillis = 30000;
	private int m_requestQueueDepth = 50000;
	private int m_numZKnodes = 1; 
	private int m_port;
	private String m_connectionStr = null;
	private ContextConfig m_cc;
	private ConcurrentHashMap<String, Long> m_changeTracker = new ConcurrentHashMap<String, Long>();
	private TransportConfig m_transportConfig;
	private static UUID unique;
	private static String s_nodename;
	private CuratorFramework m_zkhandle ;
	private List<JetstreamTopic> m_registeredTopics = new CopyOnWriteArrayList<JetstreamTopic>();
	private List<String> m_NettyDiscoverableTopics = new CopyOnWriteArrayList<String>();
	private CopyOnWriteArrayList<String> m_nettyContexts = new CopyOnWriteArrayList<String>();
	private final static String FWD_SLASH = "/";
	private Object lock = new Object();
	private LinkedBlockingQueue<Request> m_requestQueue ; 
	Thread m_reqWorker ;
	private PingerTask pingerTask;
	private AtomicInteger m_cnxnLostCount = new AtomicInteger(0);
	private Long m_prevCnxnRefreshedTimeStamp = Long.valueOf(0);
	private int m_cnxnBalanceIntervalInMs = 6 * 3600 * 1000;
	private int m_pingerNotConnectedThreshold;
	private List<String> m_nonresolvablehosts = new ArrayList<String>();
	private List<String> m_resolvablehosts = new ArrayList<String>();
	private Date m_last_Cnxn_Success = new Date();
	
	static {
			unique = UUID.randomUUID();
			s_nodename = unique.toString();
			LOGGER.warn( "Unique Node Name :" + s_nodename);
			System.setProperty("zookeeper.jmx.log4j.disable", "true");
	}

	CuratorFramework getZKHandle() {
		return m_zkhandle;
	}
	
	public List<String> getNonresolvablehosts() {
		return m_nonresolvablehosts;
	}
	
	public List<String> getResolvablehosts() {
		return m_resolvablehosts;
	}
	
	public String getLast_Cnxn_Success() {
		return m_last_Cnxn_Success.toString();
	}
	
	private void setZKHandle(CuratorFramework clientHandle){
		m_zkhandle = clientHandle;
	}
	
	public int getRequestQueueDepth(){
		return m_requestQueue.size();
	}
	
	public boolean isTransportInitialized(){
		return m_initialized.get();
	}

    public boolean isZKConnected(CuratorFramework zkClient) {
        if (zkClient != null && zkClient.getZookeeperClient() != null)
            try {
                return zkClient.getZookeeperClient().getZooKeeper().getState().isConnected();
            } catch (Exception e) {
                LOGGER.error( "Exception while retrieving connection state :", e);
                return false;
            }
        else {
            return false;
        }
    }
	   
	public boolean getConnected() {
	    return isZKConnected(m_zkhandle);
	}

	public int getCxnWaitInMillis() {
		return m_cxnWaitInMillis;
	}

	public String getConnectionStr() {
		return m_connectionStr;
	}

	public ConcurrentHashMap<String, Long> getChangeTracker() {
		return m_changeTracker;
	}

	public ZooKeeperTransport() {
		m_queueStats = new DispatchQueueStats();
		m_initialized = new AtomicBoolean(false);
		m_shutdown = new AtomicBoolean(false);
	}

	public ZooKeeperTransport(String addr, int port, ITransportListener tl) {
		m_queueStats = new DispatchQueueStats();
		m_initialized = new AtomicBoolean(false);
		m_shutdown = new AtomicBoolean(false);
		this.m_addr = null;
		this.m_addr = addr;
		this.m_port = port;
		m_transportListener = tl;
	}

	public TransportStats getStats() {

		TransportStats stats = new TransportStats();
		stats.setContext(m_context);
		stats.setProtocol("tcp");
		stats.setMsgsRcvdPerSec((long) m_msgsRcvdPerSec.get());
		stats.setMsgsSentPerSec((long) m_msgsSentPerSec.get());
		stats.setTotalMsgsRcvd(m_totalMsgsRcvd.get());
		stats.setTotalMsgsSent(m_totalMsgsSent.get());
		return stats;
	}

	public void harvestStats() {

	}

	/**
	 * Method connects to zookeeper emsemble and register the context to the
	 * transport
	 */
	public void init(TransportConfig transportConfig, NICUsage nicUsage,
			DNSMap dnsMap, MessageServiceProxy proxy) throws Exception {

		if (m_initialized.get()) {
			return;
		}
		
		//in case , if transport is re-initialised, flipping shutdown flag
		m_shutdown.set(false);
		m_transportConfig = transportConfig;

		if (transportConfig instanceof ZooKeeperTransportConfig) {
			ZooKeeperTransportConfig config = (ZooKeeperTransportConfig) transportConfig;
			m_cxnWaitInMillis = config.getCxnWaitInMillis() ;
			m_retrycount = config.getRetrycount();
			m_retrywaitTimeInMillis = config.getRetryWaitTimeInMillis();
			m_sessionTimeoutInMillis = config.getSessionTimeoutInMillis();
			m_numZKnodes = config.getZknodes().size();

			m_NettyDiscoverableTopics.addAll(config.getNettyDiscoveryProtocolTopics());
			
			m_requestQueueDepth = config.getRequestQueueDepth();

			if (!config.getZknodes().isEmpty()) {
				m_connectionStr = prepareConnectString(config.getZknodes());
			} else {
				throw new Exception("ZooKeeper Servers are NOT configured");
			}

			m_cnxnBalanceIntervalInMs = config.getCnxnBalanceIntervalInHrs() * 3600 * 1000;
			m_pingerNotConnectedThreshold = config.getPingerCnxnLostThreshold();
			
			pingerTask = new PingerTask();
			MessageServiceTimer
			.sInstance()
			.getTimer()
			.scheduleAtFixedRate(
					pingerTask,
					(long) config.getPingerIntervalInMins() * 60 * 1000,
					(long) config.getPingerIntervalInMins() * 60 * 1000);
					
		}
		
		m_requestQueue = new LinkedBlockingQueue<Request>(m_requestQueueDepth);
		LOGGER.info( "Registering Context -" + m_context);
		if (getZKHandle() == null) {
			connect();
		}

		createContext(m_context);

		Management.addBean(
				(new StringBuilder())
						.append("MessageService/Transport/ZooKeeper/context/")
						.append(m_context).append("/stats").toString(),
				new TransportStatsController(this));

		Management
				.addBean(
						(new StringBuilder())
								.append("MessageService/Transport/ZooKeeper/context/")
								.append(m_context).append("/registry")
								.toString(), this);

		m_serviceProxy = proxy;
		m_reqWorker = new Thread(this, "ZKTransportRequestWorker");
		m_reqWorker.start();
		m_initialized.set(true);
	}

	
	/**
	 * Connects to one of zookeeper servers in ensemble with wait time enabled.
	 * 
	 * @param m_cxnWaitInMillis
	 */
	void connect() throws Exception {
		CuratorFramework zkClient = null;
		long connectioncreatime = 0;
		LOGGER.warn(
				"Connection to ZooKeeper Server - ZooKeeper Server Connection Params: cnxnstr = "
						+ m_connectionStr
						+ " , connectiontimeout="
						+ m_cxnWaitInMillis
						+ ", retryCount="
						+ m_retrycount
						+ ", retryWaitTime=" + m_retrywaitTimeInMillis);
		long start = System.currentTimeMillis();
		
		zkClient = connectInternal(m_sessionTimeoutInMillis, m_cxnWaitInMillis,
				m_retrycount, m_retrywaitTimeInMillis);
		long end = System.currentTimeMillis();
		connectioncreatime = end - start;
			

		if (zkClient != null
				&& (!isZKConnected(zkClient))) {
		    zkClient.close();
			throw new Exception("Not able to connect to ZooKeeper Server");
		} else {
			setZKHandle(zkClient);
			LOGGER.warn(
					"Session Connected to ZooKeeper Server : "
							+ m_connectionStr);
			LOGGER.warn( "ZooKeeper connection creation took - "
					+ connectioncreatime + " ms");
		}
	}
	
	private  String  findDNSResolvableZkHosts(String origCnxnStr) {
	
		String[] hostporttuple = origCnxnStr.split(",");
		StringBuffer newcnxStr = new StringBuffer();
		int count = 1;
		//reset resolvable list
		m_resolvablehosts.clear();
		m_nonresolvablehosts.clear();
		for(String hostport : hostporttuple){
			String host = hostport.split(":")[0];
			String port = hostport.split(":")[1];
			try {
				InetAddress.getAllByName(host);
				if(count != 1)
					newcnxStr.append(",");
				
				newcnxStr.append(host);
				newcnxStr.append(":");
				newcnxStr.append(port);
				m_resolvablehosts.add(host);
				count++;
			} catch (UnknownHostException ukhe) {
				String trace = ExceptionUtils.getStackTrace(ukhe);
				m_nonresolvablehosts.add(host);
				LOGGER.error(" Non Resolvable HostName : " +  host + " : " + trace);
			}catch (Throwable t) {
				String trace = ExceptionUtils.getStackTrace(t);
				m_nonresolvablehosts.add(host);
				LOGGER.error(" Got other exception while resolving hostname : " +  host + " : " + trace);
			}
			
		}
		return newcnxStr.toString();
	}

	
	
	private CuratorFramework connectInternal(int sessiontimeout,
			int connectiontimeout, int retryCount, int retryWaitTime) {

		synchronized (lock) {
				CuratorFramework zkClient = null;
			    
				LOGGER.warn( "Connection String Before Hostname Resolution : " +  m_connectionStr);
			   
				String resolvablcnxnstr =  findDNSResolvableZkHosts(m_connectionStr);
				
				LOGGER.warn( "Connection String After Hostname Resolution : " +  resolvablcnxnstr);
				
				if(!resolvablcnxnstr.isEmpty()){
					zkClient = ZooKeeperConnectionUtil.createZookeeperConnection(
							resolvablcnxnstr, sessiontimeout, connectiontimeout,
							this, retryCount, retryWaitTime);
					try {
						 lock.wait((long) connectiontimeout * m_numZKnodes);
					} catch (InterruptedException ie) {
						LOGGER.error( "Received Interreptued Exception",
								ie);
						Thread.currentThread().interrupt();
					}
				} else{
					LOGGER.error( "None of the given hostnames are Resolvable : " + m_connectionStr);
				}
			return zkClient;
		}		
		
	}
	
	/**
	 * Restarting Curator Client
	 */
	void restartZkClient(CuratorFramework handle) {
	    if (handle == m_zkhandle) { // 
    		if(handle!= null && !handle.getState().equals(CuratorFrameworkState.STARTED)){
    		    handle.start();
    		}
	    }
	}
	
	/**
	 * Closing connection to ZK cluster
	 */
	private void closeConnection() {
	    getZKHandle().getConnectionStateListenable().removeListener(this);
		getZKHandle().close();
	}

	/**
	 * MeesageService exposes single address/port combination. But This
	 * transport expects ZooKeeper servers to be deployed as ensemble(group of
	 * servers for failure mgmt). So assumption is each ZooKeeper server port is
	 * in incremental order. Based on the no. of servers configured in
	 * ZooKeeperTransportConfig, port number would be increased and connections
	 * string will be formed.
	 * 
	 * Ex. addr1:port1,addr2:port2,addr3:port3
	 * 
	 * @return
	 */
	private String prepareConnectString(List<ZooKeeperNode> zknodes) {
		StringBuffer buf = new StringBuffer();
		int count = 1;
		for (ZooKeeperNode node : zknodes) {
			buf.append(node.getHostname());
			buf.append(":");
			buf.append(node.getPort());
			if (count != zknodes.size()) {
				buf.append(",");
			}
			count++;
		}
		return buf.toString();
	}

	/**
	 * Znode starts with '/' in ZK. So appending to the context/topic name
	 * before it is created in ZK.
	 * 
	 * @param path
	 * @return
	 */
	private String prependPath(String path) {
		if (path.charAt(0) != '/') {
			return (new StringBuilder()).append("/").append(path).toString();
		}
		return path;
	}

	public void pause(JetstreamTopic jetstreamtopic) {
		// flow control is not supported for this transport
	}

	/**
	 * Registering listener to post back the messages.
	 */
	public void registerListener(ITransportListener tl) throws Exception {
		m_transportListener = tl;
	}

	/**
	 * Registering the topic name to Transport. Each Topic will be created as a
	 * znode in ZK. Watch is also set after creating the node to receive
	 * notifications.
	 */
	public void registerTopic(JetstreamTopic topic) {
		if (!m_registeredTopics.contains(topic))
			m_registeredTopics.add(topic);

		LOGGER.info( "Register Topic :: " + topic.getTopicName());
		Request registerReq = new WatchTopicRequest(this, m_zkhandle, topic);
		if(!m_requestQueue.offer(registerReq))
			LOGGER.warn( "Request Queue is Full..");
	}

	public void setWatchForRegisterdTopic(JetstreamTopic topic) {
		String path = prependPath(topic.getTopicName());
		setDefaultNettyDiscoveryTopics();
		try {
			if (m_initialized.get()) {
				if (m_NettyDiscoverableTopics.contains(topic.getTopicName())) {
					List<String> subgroupPaths = pathForNettyContexts(path);
					for (String subgroupPath : subgroupPaths) {
						m_group.createsubgroups(subgroupPath);
						long start = System.currentTimeMillis();
						getSetPublishChildrenWatch(subgroupPath);
						long end = System.currentTimeMillis();
						LOGGER.warn(
								"Time taken to publish initial nodes :"
										+ (end - start) + "ms for path :"
										+ path);
					}
				} else {
					m_group.createsubgroups(path);
					long start = System.currentTimeMillis();
					String subgrouppath;
					if (!path.contains(m_group.getGroupname())) {
						subgrouppath = path.contains(FWD_SLASH) ? new StringBuffer(
								m_group.getGroupname()).append(path).toString() : new StringBuffer(
										m_group.getGroupname()).append(FWD_SLASH).append(path).toString();
						path = subgrouppath ;
					} 
					getSetPublishChildrenWatch(path);
					long end = System.currentTimeMillis();
					LOGGER.warn(
							"Time take to publish initial nodes :"
									+ (end - start) + "ms for path :" + path);
				}

			} else {
				LOGGER.error(
						"Not able to register the topic. ZooKeeper Transport is Not initialized properly. Please check the logs.");
				throw new Exception(
						"Not able to register the topic - "
								+ topic.getTopicName()
								+ ". ZooKeeper Transport is Not initialized properly. Please check the logs.");
			}

		} catch (Exception e) {
			LOGGER.error( "Exception while Registering Topic :: "
					+ path, e);
		}

	}

	private void setDefaultNettyDiscoveryTopics() {
		if (m_NettyDiscoverableTopics.isEmpty()) {
			m_NettyDiscoverableTopics
					.add(TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG);
			m_NettyDiscoverableTopics
					.add(TopicDefs.JETSTREAM_EVENT_CONSUMER_DISCOVER_MSG);
		}
	}

	private List<String> pathForNettyContexts(String path) {
		List<String> nettyContextpaths = new ArrayList<String>();
		for (String nettyContextpath : m_nettyContexts) {
			StringTokenizer tokens = new StringTokenizer(nettyContextpath, "/");
			StringBuffer tokenpath = new StringBuffer();
			while (tokens.hasMoreElements()) {
				if (tokenpath.length() != 0)
					tokenpath.append('/');

				tokenpath.append(tokens.nextToken());
				nettyContextpaths.add(path + "/" + tokenpath);
			}

		}
		return nettyContextpaths;
	}

	/**
	 * This method goes over all children and publishes the data to transport
	 * listener
	 * 
	 * @param path
	 */
	public void getSetPublishChildrenWatch(String path) {
		List<String> children = null;
		try {
			children = getZKHandle().getChildren().usingWatcher(this)
					.forPath(path);

			if (children.size() > 0) {
				for (String child : children) {
					String childnodePath = (new StringBuilder()).append(path)
							.append("/").append(child).toString();

					getZKHandle().checkExists().usingWatcher(this)
							.forPath(childnodePath);
					getSetPublishChildrenWatch(childnodePath);
				}
			} else {
				// It is an ephemeral node, so just get the data and publish it
				// immediately
				byte[] nodedata = getZKHandle().getData().usingWatcher(this)
						.forPath(path);
				ZooKeeperDataWrapper datawrapper = getWrapper(nodedata);
				if (isChanged(path, datawrapper)) {
					publishData(path, nodedata);
				}
			}

		} catch (Exception e) {
			LOGGER.error( " Exception While setting watch on "
					+ path, e);
		}
	}

	/**
	 * For given znode, data and child watch is set to receive notifications
	 * Exists watch - to receive NodeCreated, NodeDeleted, NodeDataChanged
	 * notifications Children watch = to receive NodeChildrenChanged,
	 * NodeDeleted notifications
	 * 
	 * @param path
	 * @return
	 */

	/**
	 * Reset counters
	 * 
	 */
	public void resetStats() {
		m_msgsRcvdPerSec.reset();
		m_msgsSentPerSec.reset();
		m_totalMsgsRcvd.reset();
		m_totalMsgsSent.reset();
	}

	public void resume(JetstreamTopic jetstreamtopic) {
		// flow control is not supported for this transport
	}

	/**
	 * Send Message to groups via ZK
	 */
	public void send(JetstreamMessage msg) throws Exception {
		if (msg == null) {
			throw new Exception("Null object being passed in");
		}
		Request req = new ZKSendMessageRequest(this, msg); 
		if(!m_requestQueue.offer(req))
			LOGGER.warn( "Request Queue is Full..");
	}
	
	void setData(JetstreamMessage msg){
		
		if (!m_initialized.get()) {
			//throw new Exception("ZooKeeperTransport not initialized");
		}

		ZooKeeperDataWrapper wrapperData = new ZooKeeperDataWrapper(msg);
		ByteArrayOutputStream out_stream = new ByteArrayOutputStream(64000);
		out_stream.reset();
		ObjectOutputStream out;
		try {
			out = new ObjectOutputStream(out_stream);
			out.writeObject(wrapperData);
			out.flush();
		} catch (IOException e) {
			LOGGER.warn( e.getLocalizedMessage());
		}
		
		byte buf[] = out_stream.toByteArray();
		String path = prependPath(msg.getTopic().getTopicName());

		String ctxtpath = getNettyContext(msg);
		if (ctxtpath != null) {
			path = path + prependPath(ctxtpath);
		}

		LOGGER.info( "Topic to Send ::" + path);
		try {
			m_group.setGroupMemberData(path, buf);
		} catch (Throwable t) {
			LOGGER.warn( "Exception while sending message to ZK server", t);
		}


		m_totalMsgsSent.increment();
		m_msgsSentPerSec.increment();

		buf = null;

	}

	private String getNettyContext(JetstreamMessage msg) {

		if (msg instanceof EventConsumerAdvertisement) {
			EventConsumerAdvertisement advmsg = (EventConsumerAdvertisement) msg;
			return advmsg.getContext();
		} else if (msg instanceof EventConsumerDiscover) {
			EventConsumerDiscover discovermsg = (EventConsumerDiscover) msg;
			return discovermsg.getContext();
		} else
			return null;

	}

	/**
	 * Each Topic will be created as persistent znode in ZK
	 * 
	 * @param path
	 */
	private void createContext(String path) {
		String verifiedpath = null;
		verifiedpath = prependPath(path);
		try {
			m_group = new GroupMembership(verifiedpath, s_nodename, this, this);
		} catch (Exception e2) {
			LOGGER.error( "Exception while creating the context :"
					+ path, e2);
		}
	}

	/**
	 * set ZK addr
	 */
	public void setAddr(String addr) {
		this.m_addr = addr;
	}

	public void setContextConfig(ContextConfig cc) {
		m_context = cc.getContextname();
		m_cc = cc;

	}

	public void setMessageServiceProperties(MessageServiceProperties props) {
		this.m_props = props;
		addNettyContexts(m_props);
	}

	private void addNettyContexts(MessageServiceProperties msp) {
		if (msp != null) {
			List<TransportConfig> configs = msp.getTransports();
			for (TransportConfig oneConfig : configs) {
				if (oneConfig instanceof NettyTransportConfig) {
					for (ContextConfig ctxtConfig : oneConfig.getContextList()) {
						if (!m_nettyContexts.contains(ctxtConfig
								.getContextname())) {
							m_nettyContexts.add(ctxtConfig.getContextname());
						}
					}
				}
			}

		}
	}

	public void setPort(int port) {
		this.m_port = port;
	}

	public void setUpstreamDispatchQueueStats(
			DispatchQueueStats dispatchqueuestats) {
	}

	/**
	 * Upon shutdown signal, close the connection to ZooKeeper and come out
	 */
	public void shutdown() throws MessageServiceException {
		LOGGER.warn( "Shutdown Signal Received. Shutting down.");
		
		m_shutdown.set(true);
		
		Request shutdownRequest = new AbortRequest();
		m_requestQueue.offer(shutdownRequest);
		
		//Drain all requests from queue
		while (m_requestQueue.size() != 0) {
			try {
				Thread.sleep(100);
			}catch (InterruptedException e) {
				LOGGER.error( "Exception while draining requests from queue", e);
			}
		}
		
		closeConnection();
		
		if (pingerTask != null)
			pingerTask.cancel();

		boolean status = Management
				.removeBeanOrFolder("MessageService/Transport/ZooKeeper/context/"
						+ m_context + "/stats");

		if (status)
			LOGGER.info(
					"stop monitoring MessageService/Transport/ZooKeeper/context/"
							+ m_context + "/stats");

		status = Management
				.removeBeanOrFolder("MessageService/Transport/ZooKeeper/context/"
						+ m_context + "/registry");

		if (status)
			LOGGER.info(
					"stop monitoring MessageService/Transport/ZooKeeper/context/"
							+ m_context + "/registry");

		
	}

	/**
	 * Unregister a topic when notifications are not to be received.
	 */
	public void unregisterTopic(JetstreamTopic topic) {
		Object contextList[] = topic.getContexts();
		Object arr$[] = contextList;
		int len$ = arr$.length;
		for (int i = 0; i < contextList.length; i++) {
			Object context = contextList[i];
			String path = (String) context;
			try {
				m_group.leave(prependPath(path));
			} catch (Exception e) {
				LOGGER.error(
						" Exception on leaving group :" + path, e);
			}
		}

	}

	/**
	 * Call back receiver method to process notifications on node changes.
	 */
	public void process(WatchedEvent event) {
		LOGGER.info( "Event State :: " + event.getState().name()
				+ " Event Type:: " + event.getType() + " Event path :: "
				+ event.getPath());
		
		if(m_shutdown.get()){
			LOGGER.warn( "Transport is in Shutdown State.");
			return;
		}

		switch (event.getType()) {

		case NodeChildrenChanged:
			if (m_group != null) {
				boolean isDiscoveryTopic = false ;
				for (String nettydiscoverable : m_NettyDiscoverableTopics) {
					if (event.getPath().contains(nettydiscoverable)) {
						isDiscoveryTopic = true ;
						for (String nettyconextnode : m_nettyContexts) {
							if (event.getPath().contains(nettyconextnode)) {
								m_group.registerChildrenCallback(event
										.getPath());
								m_group.getChildrenAndSetWatch(event.getPath());
								break;
							}
						}
					}
				}
				if (!isDiscoveryTopic) {
					m_group.registerChildrenCallback(event.getPath());
					m_group.getChildrenAndSetWatch(event.getPath());
				}
			}

			break;

		case NodeCreated:
			if (m_group != null) {
				m_group.registerDataCallBack(event.getPath());
			}
			break;

		case NodeDataChanged:
			if (m_group != null) {
				m_group.registerDataCallBack(event.getPath());
			}
			break;

		case NodeDeleted:
			if (m_group != null) {
				int parentIndex = event.getPath().lastIndexOf('/');
				String parentNode = event.getPath().substring(0, parentIndex);
				m_group.getChildrenAndSetWatch(parentNode);
			}
			break;

		case None:
			break;
		default:
			break;
		}

	}

	/**
	 * Post notifications on to transport listeners
	 * 
	 * @param path
	 */
	private void publishData(String path, byte[] data) {
		try {

			if (data != null && data.length > 0) {

				ByteArrayInputStream bii = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bii);
				Object dataObj = ois.readObject();
				ZooKeeperDataWrapper wrapper = (ZooKeeperDataWrapper) (dataObj);
				JetstreamMessage tm = (JetstreamMessage) wrapper
						.getOrginalData();
				String trackerKey = createKey(path, wrapper);
				m_changeTracker.put(trackerKey, wrapper.getTimestamp());
				try {
					LOGGER.warn( "Publishing the change for topic :"
							+ path + " TrackerKey :" + trackerKey
							+ " Timestamp : " + wrapper.getTimestamp());
					m_transportListener.postMessage(tm, m_queueStats);
				} catch (Throwable mse) { // don't want to kill zk thread., so catching Throwable
					StringBuffer buf = new StringBuffer();
					buf.append("Error dispatching to message service - ");
					buf.append(mse.getLocalizedMessage());

					LOGGER.error(
							"Exception publishing data for path :" + path
									+ buf.toString(), mse);
				}

				m_totalMsgsRcvd.increment();
				m_msgsRcvdPerSec.increment();

			}
		} catch (ClassNotFoundException e) {
			LOGGER.error(
					" ClassNotFoundException while publishing data to transport Listener :"
							+ path, e);
		} catch (IOException e) {
			LOGGER.error(
					" IOException while publishing data to transport Listener :"
							+ path, e);
		}
	}

	@Override
	@Hidden
	public ContextConfig getContextConfig() {
		return m_cc;
	}

	@Override
	public void processResult(CuratorFramework client, CuratorEvent event)
			throws Exception {
		
		if(m_shutdown.get()){
			LOGGER.warn( "Transport is in Shutdown State.");
			return;
		}

		if (event != null)
			if (event.getType().equals(CuratorEventType.CHILDREN)) {
				for (String child : event.getChildren()) {
					String childnodePath = (new StringBuilder())
							.append(event.getPath()).append("/").append(child)
							.toString();
					byte[] childdata = m_group.getMemberData(childnodePath);

					ZooKeeperDataWrapper datawrapper = getWrapper(childdata);
					if (isChanged(childnodePath, datawrapper)) {
						publishData(childnodePath, childdata);
					}
				}
			} else {
				ZooKeeperDataWrapper datawrapper = getWrapper(event.getData());
				if (datawrapper != null) {
					if (isChanged(event.getPath(), datawrapper)) {
						publishData(event.getPath(), event.getData());
					}
				}
			}
	}

	private ZooKeeperDataWrapper getWrapper(byte[] data) {
		try {
			if (data != null && data.length > 0) {

				try {
					InetAddresses.forString(new String(data));
					return null;
				} catch (IllegalArgumentException iae) {
					// just skip it
				}

				ByteArrayInputStream in_stream = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(in_stream);
				Object dataObj = ois.readObject();
				ZooKeeperDataWrapper wrapper = (ZooKeeperDataWrapper) (dataObj);
				return wrapper;
			}
		} catch (IllegalArgumentException iae) {
			// just skip it
		} catch (IOException e) {
			LOGGER.warn(
					"IoException while constructing wrapper : " + e);
		} catch (ClassNotFoundException e) {
			LOGGER.warn(
					"ClassNotFoundException while constructing wrapper : " + e);
		}

		return null;

	}

	private String createKey(String path, ZooKeeperDataWrapper wrapper) {
		byte[] orig = ((JetstreamMessage) wrapper.getOrginalData())
				.getMsgOrigination();
		try {
			String hostname = InetAddress.getByAddress(orig).getHostName();
			String key = hostname + "_" + path;
			return key;
		} catch (UnknownHostException e) {
			LOGGER.warn(
					"UnknownHostException while creating the key :" + path, e);
		}
		return null;
	}

	private boolean isChanged(String path, ZooKeeperDataWrapper datawrapper) {
		if (datawrapper != null) {
			String key = createKey(path, datawrapper);
			if (m_changeTracker.containsKey(key)) {
				if (m_changeTracker.get(key) != null
						&& datawrapper.getTimestamp() > m_changeTracker
								.get(key)) {
					m_changeTracker.put(key, datawrapper.getTimestamp());
					return true;
				} else
					return false;
			} else {
				m_changeTracker.put(key, datawrapper.getTimestamp());
				return true;

			}

		} else {
			return false;
		}
	}

	@Override
	public void stateChanged(CuratorFramework curator, ConnectionState state) {

		switch (state) {
		case CONNECTED:
			try {
				LOGGER.warn( "Session Connected to ZooKeeper");
				m_last_Cnxn_Success.setTime(System.currentTimeMillis());
		        synchronized (lock) {
					lock.notifyAll();
				    Request req = new WatchRegisterRequest(this, curator);
		            m_requestQueue.offer(req);
		      }
			} catch (Throwable t) {
				LOGGER.error(
						"Exception while connection to ZooKeeper", t);
			}
			break;
		case LOST:
			try {
				if(!isZKConnected(curator)){ //  we might get false alarm, hence validating the state
					LOGGER.warn(
							"Connection LOST to ZooKeeper server. Connecting again...");
					Request req = new ZKCuratorRestartRequest(this, curator);
					m_requestQueue.offer(req);
				} else{
					//ignore this message
				}
				
			} catch (Exception e) {
				LOGGER.error(
						"Exception while connection to ZooKeeper", e);
			}
			break;

		case RECONNECTED:
			LOGGER.warn( "Session Reconnected to ZooKeeper");
            Request req = new WatchRegisterRequest(this, curator);
            m_requestQueue.offer(req);
            break;

		case READ_ONLY:
			LOGGER.warn( "Connection changed READ_ONLY State");
			break;
		case SUSPENDED:
			LOGGER.warn( "Connection changed to SUSPENDED State");
			break;

		default:
			break;
		}
	}

	@Override
	public void prepareToPublish(String context) {
		// This transport doesn't need to react to this signal

	}

	@Hidden
	public MessageServiceProxy getServiceProxy() {
		return m_serviceProxy;
	}

	@Hidden
	public MessageServiceProperties getProps() {
		return m_props;
	}

	@Hidden
	public String getAddr() {
		return m_addr;
	}

	@Hidden
	public int getPort() {
		return m_port;
	}

	public TransportConfig getTransportConfig() {
		return m_transportConfig;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_cc == null) ? 0 : m_cc.hashCode());
		result = prime * result
				+ ((m_connectionStr == null) ? 0 : m_connectionStr.hashCode());
		result = prime * result
				+ ((m_context == null) ? 0 : m_context.hashCode());
		result = prime * result + m_cxnWaitInMillis;
		result = prime * result + ((m_group == null) ? 0 : m_group.hashCode());
		result = prime * result
				+ ((m_queueStats == null) ? 0 : m_queueStats.hashCode());
		result = prime * result + m_retrycount;
		result = prime
				* result
				+ ((m_transportConfig == null) ? 0 : m_transportConfig
						.hashCode());
		result = prime * result + ((m_group == null) ? 0 : m_group.hashCode());

		return result;
	}

	@Override
	public boolean equals(Object obj) {    
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ZooKeeperTransport other = (ZooKeeperTransport) obj;

		if (m_cc == null) {
			if (other.m_cc != null)
				return false;
		} else if (!m_cc.equals(other.m_cc))
			return false;

		if (m_connectionStr == null) {
			if (other.m_connectionStr != null)
				return false;
		} else if (!m_connectionStr.equals(other.m_connectionStr))
			return false;

		if (m_context == null) {
			if (other.m_context != null)
				return false;
		} else if (!m_context.equals(other.m_context))
			return false;

		if (m_cxnWaitInMillis != other.m_cxnWaitInMillis)
			return false;
		if (m_group == null) {
			if (other.m_group != null)
				return false;
		} else if (!m_group.equals(other.m_group))
			return false;

		if (m_transportConfig == null) {
			if (other.m_transportConfig != null)
				return false;
		} else if (!m_transportConfig.equals(other.m_transportConfig))
			return false;

		return true;
	}
	
	void rebalance() {
		try {
			LOGGER.warn(
					"Auto balancing ZK connections to server");
			
			CuratorFramework oldClient = getZKHandle();
			
			// establishing new connection
			CuratorFramework newClient = connectInternal(m_sessionTimeoutInMillis, m_cxnWaitInMillis, m_retrycount, m_retrywaitTimeInMillis);
			if(newClient != null && isZKConnected(newClient)){
				setZKHandle(newClient);
				//close old connection
				if(oldClient != null){
					oldClient.getConnectionStateListenable().removeListener(this);
					oldClient.close();
					oldClient = null;
				}	
			}else if(newClient!= null){
				//lets close the dangling connection & object 
				newClient.getConnectionStateListenable().removeListener(this);
				newClient.close();
				newClient = null;
			}	
			
		} catch (Throwable t) {
			String trace = ExceptionUtils.getStackTrace(t);
			LOGGER.error(
					"Exception whil rebalancing connections : " + trace );
		}
	}
	 	
	public void checkCnxnState() {
		try {
			
			if(!getConnected()){
				if(m_cnxnLostCount.incrementAndGet() > m_pingerNotConnectedThreshold){
					rebalance();
					m_cnxnLostCount.set(0);
				}
			}else{
                m_cnxnLostCount.set(0);
				long curtime = System.currentTimeMillis();
				if(curtime - m_prevCnxnRefreshedTimeStamp > m_cnxnBalanceIntervalInMs){
					rebalance();
				}	
			}
		} catch (Throwable t) {
			String trace = ExceptionUtils.getStackTrace(t);
			LOGGER.error(
					"Exception in Pinger Task" + trace);
		}
	}

	
	class PingerTask extends TimerTask {

		@Override
		public void run() {
			try{
				Request cnxnReq = new PingerRequest(ZooKeeperTransport.this);
				m_requestQueue.offer(cnxnReq);
			}catch(Throwable t){
				String trace = ExceptionUtils.getStackTrace(t);
				LOGGER.error(
						"Exception in pingerTask connections" + trace);
			}
			
		}
	}

	@Override
	public void run() {

		while (true) {
			try {
				
				Request req = m_requestQueue.take();
				if(req instanceof AbortRequest)
					return;
					
				if(req != null){
					req.execute();
				}	

			} catch (Throwable t) {
				String trace = ExceptionUtils.getStackTrace(t);
				LOGGER.error(
						"Exception in request Processing.. Proceeding with next request.." + trace);

			}

		}

	}

    public void setWatchOnZkClient(CuratorFramework handle) {
        if (handle == m_zkhandle) {
        	
        	m_prevCnxnRefreshedTimeStamp = System.currentTimeMillis();
            m_cnxnLostCount.set(0);

            // set watches for new connection
            for (JetstreamTopic topic : m_registeredTopics) {
            	setWatchForRegisterdTopic(topic);
            }

        }
    }
    
    public void setWatchOnZkClient(CuratorFramework handle, JetstreamTopic topic) {
        if (handle == m_zkhandle) { 
        		// set watches for this topic.
                setWatchForRegisterdTopic(topic);
        }
    }

}
