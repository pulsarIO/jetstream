/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

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

public class MsgSrvcsNegTest {

	public MsgSrvcsNegTest() throws Exception {
	}
	
	MessageServiceProperties msp;
	ArrayList<TransportConfig> transports;
	MessageServiceConfiguration msc;
	
	public byte[] fillByteBuf(){
		byte[] buf = new byte[100];
		for (int i = 0; i < 100; i++) {
			buf[i] = (byte) i;
		}
		
		return buf;
	}

	
	public NettyTransportConfig useNettyModAffScheduler(boolean compression){
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
		nettyconfig1.setEnableCompression(compression);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		
		return nettyconfig1;
	}
	
	public NettyTransportConfig useNettyAffinityScheduler(boolean compression){
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
		nettyconfig1.setEnableCompression(compression);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		
		return nettyconfig1;
	}
	
	
	public void runSecTest(int scheduler, boolean compression){
		 
		msp= NettyTestUtils.getMsp();
		transports = new ArrayList<TransportConfig>();
		try {
			transports.add(NettyTestUtils.useZookeeper());
		} catch (Exception e3) {
			e3.printStackTrace();
		}

		switch(scheduler){
		case 1:
			//"ConsistentHashingAffinityScheduler"
			System.out.println("Now adding Rtbdpod.Rtd with consistent hashing scheduler");
			transports.add(useNettyAffinityScheduler(compression));
			break;
			
		case 2:
			//"ModuloAffinityScheduler"
			System.out.println("Now adding Rtbdpod.Rtd with modulo hashing scheduler");
			transports.add(useNettyModAffScheduler(compression));
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
		
		MsgListener listener = new MsgListener(
				bm);		


		JetstreamTopic topic = new JetstreamTopic();
		topic.setTopicName("Rtbdpod.Rtd/txn");
		
		try {
			MessageService.getInstance().subscribe(topic, listener);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}

		try {
			Thread.sleep(100); // give a sec for listener to be discovered by
								// producer.
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		MessageService.getInstance().prepareToPublish(topic);
		try {
			for (int i=0;i<100;i++)
				MessageService.getInstance().publish(topic, bm);
			Thread.sleep(1000);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}
		
		assertEquals(0, listener.getCount());
		
		try {
			MessageService.getInstance().unsubscribe(topic, listener);
			Thread.sleep(1000);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}
		
		
		try {
			MessageService.getInstance().shutDown();
		} catch (IllegalStateException e) {
			assertTrue("Test Failed - failed to shutdown : " + e.getLocalizedMessage(), true);
		}
		
		MessageService.getInstance().prepareToPublish(topic);
		try {
			for (int i=0;i<100;i++)
				MessageService.getInstance().publish(topic, bm);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}
		
		try {
			Thread.sleep(50);
			assertEquals(0, listener.getCount());
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		
		try {
			MessageService.getInstance().shutDown();
		} catch (IllegalStateException e) {
			assertTrue("Test Failed - failed to shutdown : " + e.getLocalizedMessage(), true);
		}
		transports.clear();
		
	}

	@Test
	public void runTestConsistentHashAffSch(){
		//"ConsistentHashingAffinityScheduler"
		runSecTest(1, false);
	}
	
	@Test
	public void runTestModAffSch(){
		//"ModuloAffinityScheduler"
		runSecTest(2, false);
	}
}