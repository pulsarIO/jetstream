/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.NICUsage;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.MessageServiceConfiguration;
import com.ebay.jetstream.messaging.config.MessageServiceProperties;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.messagetype.BytesMessage;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
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
public class MessageServiceWithoutDNSTest {

	public static class Listener implements IMessageListener {

		private BytesMessage m_bm;
		private AtomicInteger m_count = new AtomicInteger(0);

		public int getCount() {
			return m_count.get();
		}

		public void setCount(int count) {
			m_count.set(count);
		}

		public Listener(BytesMessage bm) {
			m_bm = bm;
		}

		@Override
		public void onMessage(JetstreamMessage m) {

			if (m instanceof BytesMessage) {

				byte[] buf = ((BytesMessage) m).getByteMessage();

				if (buf.length == m_bm.getByteMessage().length) {
					byte[] buf1 = m_bm.getByteMessage();

					for (int i = 0; i < buf.length; i++) {
						if (buf[i] != buf1[i]) {
							System.out.println("unexpected message received"); //KEEPME
							assertEquals(true, buf[i] != buf1[i]);
						}
					}
					System.out.println("expected message received"); //KEEPME
				}

				m_count.addAndGet(1);

			}

		}

	}

	public MessageServiceWithoutDNSTest() {

	}

	public void main(String[] args) throws Exception {



	}

	@Test
	public void runTest() {

		System.out.println("Starting Message Service Test without Spring and without DNS"); //KEEPME

		
		// first create an instance of the NICUsage bean - this bean resolves the interface by matching the netmask
		// to the interface IP address. 
		
		NICUsage nicusage = null;
		
		try {
			nicusage = new NICUsage();
		} catch (ConfigException e) {

			assertTrue(e.getLocalizedMessage(), true);
		}

			
		// now create MessageServiceProperties bean
		MessageServiceProperties msp = new MessageServiceProperties();

		msp.setNicUsage(nicusage); 
		
		// Now create a list of transport instances to register with MessageServiceProperties

		ArrayList<TransportConfig> transports = new ArrayList<TransportConfig>();

		transports.add(NettyTestUtils.useZookeeper());
		
		// next lets create netty transport config

		NettyTransportConfig nettyconfig = new NettyTransportConfig();
		nettyconfig.setProtocol("tcp");
		nettyconfig.setRequireDNS(false);
		nettyconfig.setNetmask("127.0.0.1/32");  // this specifies the interface over which messages will flow.
		
		ArrayList<ContextConfig> nettycontexts = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc = new NettyContextConfig();
		nettycc.setContextname("Rtbdpod.Rtd");
		nettycc.setPort(15593);    // TCP port number
		
		nettycontexts.add(nettycc);
		nettyconfig.setContextList(nettycontexts);
		nettyconfig
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig.setTransportName("netty");
		nettyconfig.setAsyncConnect(true);

		transports.add(nettyconfig);

		try {
			msp.setTransports(transports); // register the transport list with MessageServiceProperties
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}
		// Now register the MessageServiceProperties with MessageServiceConfiguration bean which will initialize the service and
		// bring up all the transport instances.
		
		MessageServiceConfiguration msc = new MessageServiceConfiguration();

		try {
			msc.setMessageServiceProperties(msp);
		} catch (Exception e) {
			assertTrue(e.getLocalizedMessage(), true);
		}

		// now create a listener and register with message service

		BytesMessage bm = new BytesMessage();
		byte[] buf = new byte[10];
		for (int i = 0; i < 10; i++) {
			buf[i] = (byte) i;
		}
		bm.setByteMessage(buf);

		MessageServiceWithoutDNSTest.Listener listener = new MessageServiceWithoutDNSTest.Listener(
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
			Thread.sleep(1000); // give a sec for listener to be discovered by
								// producer.
		} catch (InterruptedException e1) {
			
			e1.printStackTrace();
		}

		// next publish a message

		try {
			MessageService.getInstance().prepareToPublish(topic);
			MessageService.getInstance().publish(topic, bm);
		} catch (Throwable t) {
			assertTrue(t.getLocalizedMessage(), true);
		}

		int count=20;
		while (listener.getCount() <= 0) {
			try {
				Thread.sleep(100);
				if (count-- == 0) break;
			} catch (InterruptedException e) {
				assertTrue("Test Failed : " + e.getLocalizedMessage(), true);
			}
		}
		
		transports.add(NettyTestUtils.useZookeeper());

		// now remove the netty trasport, change scheduler in netty transport
		// and add it back.

		System.out.println("Now removing Rtbdpod.Rtd"); //KEEPME

		MessageServiceProperties msp1 = new MessageServiceProperties();

		msp1.setNicUsage(nicusage);
		
		try {
			msp1.setTransports(transports);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}
		
		try {
			MessageService.getInstance().setMessageServiceProperties(msp1);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}

		NettyTransportConfig nettyconfig1 = new NettyTransportConfig();
		nettyconfig1.setProtocol("tcp");

		ArrayList<ContextConfig> nettycontexts1 = new ArrayList<ContextConfig>();
		NettyContextConfig nettycc1 = new NettyContextConfig();
		nettycc1.setContextname("Rtbdpod.Rtd");
		nettycc1.setPort(15593);  
		
		Scheduler scheduler = new com.ebay.jetstream.messaging.transport.netty.eventscheduler.ConsistentHashingAffinityScheduler();
		nettycc1.setScheduler(scheduler);
		nettycontexts1.add(nettycc1);
		nettyconfig1.setContextList(nettycontexts1);
		nettyconfig1
				.setTransportClass("com.ebay.jetstream.messaging.transport.netty.NettyTransport");
		nettyconfig1.setTransportName("netty");
		nettyconfig1.setAsyncConnect(true);
		nettyconfig1.setRequireDNS(false);
		nettyconfig1.setNetmask("127.0.0.1/32");  // this would have come from DNS through servicemcast TXT record entry if using DNS
		
		transports = new ArrayList<TransportConfig>();
		transports.add(NettyTestUtils.useZookeeper());
		transports.add(nettyconfig1);

		MessageServiceProperties msp2 = new MessageServiceProperties();

		System.out
				.println("Now adding Rtbdpod.Rtd with consistent hashing scheduler"); //KEEPME
		
		msp2.setNicUsage(nicusage);
		
		try {
			msp2.setTransports(transports);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}
		
		try {
			MessageService.getInstance().setMessageServiceProperties(msp2);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}

		try {
			Thread.sleep(1000); // give a sec for listener to be discovered by
								// producer.
		} catch (InterruptedException e1) {
			
			e1.printStackTrace();
		}

		bm.setAffinityKey(Long.valueOf(2));
		listener.setCount(0);

		try {
			MessageService.getInstance().prepareToPublish(topic);
			MessageService.getInstance().publish(topic, bm);
		} catch (MessageServiceException e) {
			assertTrue(e.getLocalizedMessage(), true);
		} catch (Exception e1) {
			assertTrue(e1.getLocalizedMessage(), true);
		}

		count=20;
		while (listener.getCount() <= 0) {
			try {
				Thread.sleep(100);
				if (count-- == 0) break;
			} catch (InterruptedException e) {
				assertTrue("Test Failed : " + e.getLocalizedMessage(), true);
			}
		}
		try {
			MessageService.getInstance().shutDown();
		} catch (Exception e) {
			assertTrue("Test Failed - failed to shutdown : " + e.getLocalizedMessage(), true);
		}

	}
}
