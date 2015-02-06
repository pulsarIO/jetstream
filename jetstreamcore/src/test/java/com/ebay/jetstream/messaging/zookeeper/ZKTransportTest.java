/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.zookeeper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;

import com.ebay.jetstream.config.ConfigChangeMessage;
import com.ebay.jetstream.messaging.DispatchQueueStats;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.ITransportListener;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.topic.TopicDefs;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperNode;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperTransport;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperTransportConfig;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingCluster;

public class ZKTransportTest extends TestCase{
	
	static String cnxnString ;
	TestingCluster cluster;
	ZooKeeperTransport zkTransport ;
	String contextname = "test.Messaging";
	
	static{
		System.setProperty("zookeeper.jmx.log4j.disable", "true");
	}
	
	@Override
	protected void setUp() throws Exception {
		
		zkTransport = new ZooKeeperTransport();
		cluster = new TestingCluster(5);
		cnxnString = cluster.getConnectString();
		cluster.start();
		
		ContextConfig ccConfig = new ContextConfig();
		ccConfig.setContextname(contextname);
		zkTransport.setContextConfig(ccConfig);
		
		System.out.println("Test server Started :");
		
	}
	
	@Override
	@AfterClass
	protected void tearDown() throws Exception {
		zkTransport.shutdown();
		cluster.close();
	}
	
	
	public void testBasicFunctionality() throws Exception{
		
		// validating init/connection
		zkTransport.init(getTransportConfig(), null, null, null);
		zkTransport.registerListener(new MessageListener());
		assertTrue(zkTransport.getConnected());
		assertNotNull(getTransportConnection().checkExists().forPath("/" + contextname));
		assertTrue(zkTransport.isTransportInitialized());
		
		
		// validating topic registration
		JetstreamTopic topic = new JetstreamTopic("ecd");
		zkTransport.registerTopic(topic);
		//assertNotNull(getTransportConnection().checkExists().forPath("/" + contextname + "/" + "ecd"));
		
		// Validating default netty topic registration
		JetstreamTopic topic1 = new JetstreamTopic(TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern());
		zkTransport.registerTopic(topic1);
		//assertNotNull(getTransportConnection().checkExists().forPath("/" + contextname + "/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern()));
		
		
		// send data valiadation - default nettytopic
		zkTransport.send(getMessage("Rtbdpod.RTD"));
		
		// send data valiadation - non-netty topic 
		zkTransport.send(getConfigChangeMessage());
		
		Thread.sleep(1000);
		
		assertNotNull(getTransportConnection().checkExists().forPath("/" + "Rtbdpod.local"));
		assertNotNull(getTransportConnection().checkExists().forPath("/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern()));
		assertNotNull(getTransportConnection().checkExists().forPath("/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern() + "/" + "Rtbdpod.RTD" ) );
		
		
		// Test watches
		
	}
	
	public void testCnxnLoss() throws Exception{
		
		zkTransport.init(getTransportConfig(), null, null, null);
		zkTransport.registerListener(new MessageListener());
		
		// Validating default netty topic registration
		JetstreamTopic topic1 = new JetstreamTopic(TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern());
		zkTransport.registerTopic(topic1);
		//assertNotNull(getTransportConnection().checkExists().forPath("/" + contextname + "/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern()));
		
		
		// send data valiadation - default nettytopic
		zkTransport.send(getMessage("Rtbdpod.RTD"));
		
		Thread.sleep(1000);
		assertNotNull(getTransportConnection().checkExists().forPath("/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern()));
		assertNotNull(getTransportConnection().checkExists().forPath("/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern() + "/" + "Rtbdpod.RTD" ) );
		
		// find out current connecting server
		ZooKeeper client = getTransportConnection().getZookeeperClient().getZooKeeper();
		InstanceSpec currentserver = cluster.findConnectionInstance(client);

		// Killing current connection. Expecting to connect to new server in the ensemble
		 cluster.killServer(currentserver);
		 
		 Thread.sleep(2000);
		 
		 assertTrue(zkTransport.getConnected());
		 InstanceSpec serverAfterReconnect = cluster.findConnectionInstance(client);
		 assertFalse(serverAfterReconnect.equals(currentserver));
		 
		// test send( data valiadation - after connection loss
		long prevstat = zkTransport.getStats().getTotalMsgsRcvd();
		zkTransport.send(getMessage("Rtbdpod.RTD"));
		Thread.sleep(1000);
		assertNotNull(getTransportConnection().checkExists().forPath("/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern()));
		assertNotNull(getTransportConnection().checkExists().forPath("/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern() + "/" + "Rtbdpod.RTD" ) );
		
		assertFalse(zkTransport.getStats().getTotalMsgsRcvd() == prevstat);
		
	}
	
	public void testChangeTrancker() throws Exception{
		
		zkTransport.init(getTransportConfig(), null, null, null);
		zkTransport.registerListener(new MessageListener());
		
		// Validating default netty topic registration
		JetstreamTopic topic1 = new JetstreamTopic(TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern());
		zkTransport.registerTopic(topic1);
		
		// send data validation - default nettytopic
		zkTransport.send(getMessage("Rtbdpod.RTD")); //send 1st time
		
		Thread.sleep(1000);
		
		assertNotNull(getTransportConnection().checkExists().forPath("/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern()));
		assertNotNull(getTransportConnection().checkExists().forPath("/" + TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG.intern() + "/" + "Rtbdpod.RTD" ) );
		
		
		assertEquals(1, zkTransport.getStats().getTotalMsgsRcvd()); // expecting it should be published once
		assertEquals(1, zkTransport.getStats().getTotalMsgsSent()); // expecting it should be published once
		
		
		zkTransport.resetStats();
		
		zkTransport.send(getMessage("Rtbdpod.RTD")); // send after 1 seconds
		
		Thread.sleep(1000); // lets sleep for 1 sec to receive change notifications
		
		assertEquals(1, zkTransport.getStats().getTotalMsgsSent());
		assertEquals(1, zkTransport.getStats().getTotalMsgsRcvd());
	}
	
	
	private CuratorFramework getTransportConnection() throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, SecurityException, NoSuchMethodException{
		CuratorFramework framework ;
		
		Method method = zkTransport.getClass().getDeclaredMethod("getZKHandle");
		method.setAccessible(true);
		framework = (CuratorFramework) method.invoke(zkTransport, (Object[]) null);
		return framework;
	}
	
	
	private JetstreamMessage getMessage(String msgcontext){
		
			List<JetstreamTopic> topics = new ArrayList<JetstreamTopic>();
			topics.add(new JetstreamTopic("Rtbdpod.Messaging/EventConsumerAdvertisement"));
			EventConsumerAdvertisement data = new EventConsumerAdvertisement(8080,
					"hostnmae", "Rtbdpod.Messaging", topics);
			data.setTopic(new JetstreamTopic("Rtbdpod.Messaging/EventConsumerAdvertisement"));
			data.setContext(msgcontext);
			
			return data;
	
	}
	
	private JetstreamMessage getConfigChangeMessage(){
		
		ConfigChangeMessage data = new ConfigChangeMessage();
		data.setTopic(new JetstreamTopic("Rtbdpod.local"));
		
		return data;
		
	}
	
	private static List<ZooKeeperNode> getZkNodesFromCnxnStr(String cnxnStr){
		List<ZooKeeperNode> nodes = new ArrayList<ZooKeeperNode>();
		String[] serverports = cnxnStr.split(",");
		for(String serverport : serverports){
			String[] details = serverport.split(":");
			ZooKeeperNode node = new ZooKeeperNode();
			node.setHostname(details[0]); // hostname
			node.setPort(Integer.valueOf(details[1]));
			nodes.add(node);
		}
		System.out.println("nodes :  "  + nodes);
		return nodes;
	}
	
	
	private static TransportConfig getTransportConfig(){
		ZooKeeperTransportConfig config = new ZooKeeperTransportConfig();
		config.setCxnWaitInMillis(10000);
		config.setRetrycount(2);
		config.setRetryWaitTimeInMillis(1000);
		List<ZooKeeperNode> listnodes = getZkNodesFromCnxnStr(cnxnString);
		config.setZknodes(listnodes);
		return config;
	}
	
	class MessageListener implements ITransportListener{

		@Override
		public void postMessage(JetstreamMessage tm, DispatchQueueStats stats)
				throws MessageServiceException {
			
		}

		@Override
		public void postAdvise(JetstreamMessage tm) {
			
			
		}

        @Override
        public void postMessage(List<JetstreamMessage> msgs, DispatchQueueStats m_queueStats)
                throws MessageServiceException {
            
        }
		
	}
	

}
