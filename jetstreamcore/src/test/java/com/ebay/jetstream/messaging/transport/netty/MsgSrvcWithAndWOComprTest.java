/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty;




import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.MessageServiceConfiguration;
import com.ebay.jetstream.messaging.config.MessageServiceProperties;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.messagetype.BytesMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.config.NettyContextConfig;
import com.ebay.jetstream.messaging.transport.netty.config.NettyTransportConfig;
import com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler;

/**
 * Junit Test Case for Messaging without spring config
 * 
 * @author shmurthy
 * 
 */
@SuppressWarnings("restriction")
public class MsgSrvcWithAndWOComprTest implements MsgTestInterface{

	static AtomicInteger globalcount = new AtomicInteger(0);
	MessageServiceProperties msp;
	Random rand = new SecureRandom();
	public static void incCount() {
		globalcount.incrementAndGet();
	}
	
	public static int getCount() {
		return globalcount.get();
	}
	
	
	public MsgSrvcWithAndWOComprTest() throws Exception {
	}
	
	public static NettyTransportConfig useNettyDiffContext(){
		NettyTransportConfig nettyconfig = new NettyTransportConfig();
		nettyconfig.setProtocol("tcp");

		ArrayList<ContextConfig> nettycontexts = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc = new NettyContextConfig();
		nettycc.setContextname("Rtbd.Yooo");
		nettycc.setPort(15594);
		
		nettycontexts.add(nettycc);
		nettyconfig.setContextList(nettycontexts);
		nettyconfig
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig.setTransportName("netty");
		nettyconfig.setAsyncConnect(true);
		nettyconfig.setRequireDNS(false); 
		//nettyconfig.setNetmask("10.0.0.0/8");

		return nettyconfig;	
	}
	
	
		
	ArrayList<TransportConfig> transports;
	MessageServiceConfiguration msc;
	
	public byte[] fillByteBuf(){
		byte[] buf = new byte[100];
		for (int i = 0; i < 100; i++) {
			buf[i] = (byte) (rand.nextInt() *i);
		}
		
		return buf;
	}
	

	
	public NettyTransportConfig useNettyAffinityScheduler(){
		NettyTransportConfig nettyconfig1 = new NettyTransportConfig();
		nettyconfig1.setProtocol("tcp");

		ArrayList<ContextConfig> nettycontexts1 = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc1 = new NettyContextConfig();
		nettycc1.setContextname("Rtbdpod.Rtd");
		nettycc1.setPort(15593);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"WeightedRoundRobinScheduler");
		assertFalse(nettycc1.getScheduler().supportsAffinity());
		
		Scheduler scheduler = new com.ebay.jetstream.messaging.transport.netty.eventscheduler.ConsistentHashingAffinityScheduler();
		nettycc1.setScheduler(scheduler);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"ConsistentHashingAffinityScheduler");
		assertTrue(nettycc1.getScheduler().supportsAffinity());
		
		nettycontexts1.add(nettycc1);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		nettyconfig1.setEnableCompression(true);
		
		return nettyconfig1;
	}
	
	public NettyTransportConfig useNetty(){
		NettyTransportConfig nettyconfig1 = new NettyTransportConfig();
		nettyconfig1.setProtocol("tcp");

		ArrayList<ContextConfig> nettycontexts1 = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc1 = new NettyContextConfig();
		nettycc1.setContextname("Rtbdpod.Rtd");
		nettycc1.setPort(15593);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"WeightedRoundRobinScheduler");
		assertFalse(nettycc1.getScheduler().supportsAffinity());
		
		nettyconfig1.setEnableCompression(true);
		nettycontexts1.add(nettycc1);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		
		return nettyconfig1;
	}
	
	public NettyTransportConfig useNettyWRandomConnScheduler(){
		NettyTransportConfig nettyconfig1 = new NettyTransportConfig();
		nettyconfig1.setProtocol("tcp");

		ArrayList<ContextConfig> nettycontexts1 = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc1 = new NettyContextConfig();
		nettycc1.setContextname("Rtbdpod.Rtd");
		nettycc1.setPort(15593);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"WeightedRoundRobinScheduler");
		assertFalse(nettycc1.getScheduler().supportsAffinity());
		
		Scheduler scheduler = new com.ebay.jetstream.messaging.transport.netty.eventscheduler.WeightedRandomConnectionScheduler();
		nettycc1.setScheduler(scheduler);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"WeightedRandomConnectionScheduler");
		assertFalse(nettycc1.getScheduler().supportsAffinity());
		
		nettycontexts1.add(nettycc1);
		nettyconfig1.setEnableCompression(true);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		
		return nettyconfig1;
	}
	
	public NettyTransportConfig useNettyWRandomScheduler(){
		NettyTransportConfig nettyconfig1 = new NettyTransportConfig();
		nettyconfig1.setProtocol("tcp");

		ArrayList<ContextConfig> nettycontexts1 = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc1 = new NettyContextConfig();
		nettycc1.setContextname("Rtbdpod.Rtd");
		nettycc1.setPort(15593);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"WeightedRoundRobinScheduler");
		assertFalse(nettycc1.getScheduler().supportsAffinity());
		
		Scheduler scheduler = new com.ebay.jetstream.messaging.transport.netty.eventscheduler.WeightedRandomScheduler();
		nettycc1.setScheduler(scheduler);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"WeightedRandomScheduler");
		assertFalse(nettycc1.getScheduler().supportsAffinity());
		
		nettycontexts1.add(nettycc1);
		nettyconfig1.setEnableCompression(true);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		
		return nettyconfig1;
	}
	
	
	public NettyTransportConfig useNettyModAffScheduler(){
		NettyTransportConfig nettyconfig1 = new NettyTransportConfig();
		nettyconfig1.setProtocol("tcp");

		ArrayList<ContextConfig> nettycontexts1 = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc1 = new NettyContextConfig();
		nettycc1.setContextname("Rtbdpod.Rtd");
		nettycc1.setPort(15593);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"WeightedRoundRobinScheduler");
		assertFalse(nettycc1.getScheduler().supportsAffinity());
		
		Scheduler scheduler = new com.ebay.jetstream.messaging.transport.netty.eventscheduler.ModuloAffinityScheduler();
		nettycc1.setScheduler(scheduler);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"ModuloAffinityScheduler");
		assertTrue(nettycc1.getScheduler().supportsAffinity());
		
		nettycontexts1.add(nettycc1);
		nettyconfig1.setEnableCompression(true);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		
		return nettyconfig1;
	}
		
	@Test
	public void runDefaultSch() throws InterruptedException {
		
		msp= NettyTestUtils.getMsp();
		transports = new ArrayList<TransportConfig>();
		// first create zookeeper transport and add it to list of transports
		transports.add(NettyTestUtils.useZookeeper());

		// next lets create netty transport config
		// now add netty transport to list of transports
		transports.add(useNettyDiffContext());
		transports.add(useNetty());

		// now add transports to Message Servide Porpoerties
				
		try {
			msp.setTransports(transports);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}

		msc = new MessageServiceConfiguration();

		try {
			msc.setMessageServiceProperties(msp);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}

		try {
			MessageService.getInstance().setMessageServiceProperties(msp);
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		// now create a listener and register with message service

		BytesMessage bm = new BytesMessage();
		bm.setByteMessage(fillByteBuf());

		MsgListener listener11 = new MsgListener();
		MsgListener listener12 = new MsgListener();
		MsgListener listener21 = new MsgListener();
		MsgListener listener22 = new MsgListener();

		JetstreamTopic c1topic1 = new JetstreamTopic();
		c1topic1.setTopicName("Rtbdpod.Rtd/txn");
		
		JetstreamTopic c1topic2 = new JetstreamTopic();
		c1topic2.setTopicName("Rtbdpod.Rtd/pal");
		
		JetstreamTopic c2topic1 = new JetstreamTopic();
		c2topic1.setTopicName("Rtbd.Yooo/txn");
		
		JetstreamTopic c2topic2 = new JetstreamTopic();
		c2topic2.setTopicName("Rtbd.Yooo/pal");
		
		try {
			MessageService.getInstance().subscribe(c1topic1, listener11);
			MessageService.getInstance().subscribe(c1topic2, listener12);
			MessageService.getInstance().subscribe(c2topic1, listener21);
			MessageService.getInstance().subscribe(c2topic2, listener22);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}		
		
		try {
			MessageService.getInstance().prepareToPublish(c1topic1);
			Thread.sleep(100);
			for (int i=0;i<100;i++)
				MessageService.getInstance().publish(c1topic1, bm);
			
			MessageService.getInstance().prepareToPublish(c1topic2);
			Thread.sleep(100);
			for (int i=0;i<90;i++)
				MessageService.getInstance().publish(c1topic2, bm);
			
			MessageService.getInstance().prepareToPublish(c2topic1);
			Thread.sleep(100);
			for (int i=0;i<80;i++)
				MessageService.getInstance().publish(c2topic1, bm);
			
			MessageService.getInstance().prepareToPublish(c2topic2);
			Thread.sleep(100);
			for (int i=0;i<70;i++)
				MessageService.getInstance().publish(c2topic2, bm);
			
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}

		try {
			Thread.sleep(300);
		} catch (InterruptedException e2) {
			e2.printStackTrace();
		}
		
		assertEquals(100, listener11.getCount());
		assertEquals(90,  listener12.getCount());
		assertEquals(80,  listener21.getCount());
		assertEquals(70,  listener22.getCount());

		try {
			MessageService.getInstance().shutDown();
		} catch (IllegalStateException e) {
			assertTrue("Test Failed - failed to shutdown : " + e.getLocalizedMessage(), true);
		}
	}
	
	
	public void runSecTest(int scheduler){

		msp= NettyTestUtils.getMsp();
		transports = new ArrayList<TransportConfig>();
		transports.add(NettyTestUtils.useZookeeper());
		
		switch(scheduler){
		case 1:
			//"WeightedRoundRobinScheduler"
			System.out.println("Now adding Rtbdpod.Rtd with WeightedRoundRobinScheduler");
			transports.add(useNetty());
			break;
			
		case 2:
			//"WeightedRandomConnectionScheduler"
			System.out.println("Now adding Rtbdpod.Rtd with WeightedRandomConnectionScheduler");
			transports.add(useNettyWRandomConnScheduler());
			break;
			
		case 3:
			//"WeightedRandomScheduler"
			System.out.println("Now adding Rtbdpod.Rtd with WeightedRandomScheduler");
			transports.add(useNettyWRandomScheduler());
			break;
		
		case 4:
			//"ConsistentHashingAffinityScheduler"
			System.out.println("Now adding Rtbdpod.Rtd with consistent hashing scheduler");
			transports.add(useNettyAffinityScheduler());
			break;
			
		case 5:
			//"ModuloAffinityScheduler"
			System.out.println("Now adding Rtbdpod.Rtd with modulo hashing scheduler");
			transports.add(useNettyModAffScheduler());
			break;
		}

		try {
			msp.setTransports(transports);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}
		
		msc = new MessageServiceConfiguration();

		try {
			msc.setMessageServiceProperties(msp);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}


		BytesMessage bm = new BytesMessage();
		bm.setByteMessage(fillByteBuf());
		bm.setAffinityKey(Long.valueOf(2));

		MsgListener listener11 = new MsgListener();

		JetstreamTopic c1topic1 = new JetstreamTopic();
		c1topic1.setTopicName("Rtbdpod.Rtd/txn");

		
		
		try {
			MessageService.getInstance().subscribe(c1topic1, listener11);
			Thread.sleep(500); // give a sec for listener to be discovered by
			// producer.
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}
		
		try {
			MessageService.getInstance().prepareToPublish(c1topic1);
			Thread.sleep(500);
			for (int i=0;i<100;i++)
				MessageService.getInstance().publish(c1topic1, bm);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}

		try {
			Thread.sleep(500);
		} catch (InterruptedException e2) {
			e2.printStackTrace();
		}
		
		assertEquals(100, listener11.getCount());
		
		try {
			MessageService.getInstance().shutDown();
		} catch (IllegalStateException e) {
			assertTrue("Test Failed - failed to shutdown : " + e.getLocalizedMessage(), true);
		}
		transports.clear();
	}
	
	
	@Test
	public void runRoundRobinSch(){
		//"WeightedRoundRobinScheduler"
		runSecTest(1);
	}

	@Test
	public void runWtRandomConnSch(){
		//"WeightedRandomConnectionScheduler"
		runSecTest(2);
	}

	@Test
	public void runWeightRandomSch(){
		//"WeightedRandomScheduler"
		runSecTest(3);
	}

	@Test
	public void runConsistentHashAffSch(){
		//"ConsistentHashingAffinityScheduler"
		runSecTest(4);
	}
	
	@Test
	public void runModAffSch(){
		//"ModuloAffinityScheduler"
		runSecTest(5);
	}

}