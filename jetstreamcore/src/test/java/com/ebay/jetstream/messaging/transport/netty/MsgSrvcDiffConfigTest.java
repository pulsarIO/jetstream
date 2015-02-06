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

public class MsgSrvcDiffConfigTest implements MsgTestInterface{

	MessageServiceProperties msp;
	ArrayList<TransportConfig> transports;
	MessageServiceConfiguration msc;
	
	public MsgSrvcDiffConfigTest() throws Exception {
		
	}
	
	public byte[] fillByteBuf(){
		String s="";
		for (int i = 0; i < 10000; i++) {
			s += i ;
		}
		
		return s.getBytes();
	}
	
	
	public void runTest(boolean compression) throws Exception {
		
		msp= NettyTestUtils.getMsp();
		transports = new ArrayList<TransportConfig>();
		transports.add(NettyTestUtils.useZookeeper());

		// next lets create netty transport config
		// now add netty transport to list of transports
		NettyTransportConfig nettyconfig1 = new NettyTransportConfig();
		nettyconfig1.setProtocol("tcp");

		ArrayList<ContextConfig> nettycontexts1 = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc1 = new NettyContextConfig();
		nettycc1.setContextname("Rtbdpod.Rtd");
		nettycc1.setPort(15593);
		
		assertEquals(nettycc1.getScheduler().getClass().getSimpleName(),"WeightedRoundRobinScheduler");
		assertFalse(nettycc1.getScheduler().supportsAffinity());
		
		nettyconfig1.setEnableCompression(compression);
		nettycontexts1.add(nettycc1);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		nettyconfig1.setAutoFlushSz(1024);
		transports.add(nettyconfig1);

		// now add transports to Message Servide Porpoerties
		msp.setTransports(transports);
		

		msc = new MessageServiceConfiguration();

		try {
			msc.setMessageServiceProperties(msp);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}

		// now create a listener and register with message service

		BytesMessage bm = new BytesMessage();
		bm.setByteMessage(fillByteBuf());
		MsgListener listener = new MsgListener();
		
		JetstreamTopic topic = new JetstreamTopic();
		topic.setTopicName("Rtbdpod.Rtd/txn");

		try {
			MessageService.getInstance().subscribe(topic, listener);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}


		Thread.sleep(100); // give a sec for listener to be discovered by

		MessageService.getInstance().prepareToPublish(topic);
		
		try {
			Thread.sleep(100);
			for (int i=0;i<100;i++)
				MessageService.getInstance().publish(topic, bm);
		} catch (MessageServiceException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Thread.sleep(20000);
		System.out.println("count:"+listener.getCount());
		assertEquals(100, listener.getCount());
		
		
		try {
			MessageService.getInstance().unsubscribe(topic, listener);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}
		
				
		MessageService.getInstance().prepareToPublish(topic);
		try {
			Thread.sleep(100);
			for (int i=0;i<100;i++)
				MessageService.getInstance().publish(topic, bm);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}
		
		try {
			Thread.sleep(100);
			assertEquals(100, listener.getCount());
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		try {
			MessageService.getInstance().shutDown();
		} catch (IllegalStateException e) {
			assertTrue("Test Failed - failed to shutdown : " + e.getLocalizedMessage(), true);
		}

	}
	
	@Test
	public void runTestWithCompr(){
		try {
			runTest(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Test
	public void runTestNoCompr(){
		try {
			runTest(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}