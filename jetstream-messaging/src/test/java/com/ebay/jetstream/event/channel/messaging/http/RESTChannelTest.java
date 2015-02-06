/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging.http;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ebay.jetstream.config.RootConfiguration;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.messaging.http.inbound.InboundRESTChannel;
import com.ebay.jetstream.event.channel.messaging.http.outbound.OutboundRESTChannel;
import com.ebay.jetstream.http.netty.server.HttpServer;

public class RESTChannelTest  {

  public static class InboundChannelSink implements EventSink {

	private JetstreamEvent event;
	private AtomicInteger count = new AtomicInteger(0);
	  
	public int getCount() {
		return count.get();
	}

	public InboundChannelSink(JetstreamEvent event) {
	   this.event = event;	
	}
	
	@Override
	public String getBeanName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void sendEvent(JetstreamEvent event) throws EventException {
		 System.out.println(event.toString()); //KEEPME
		 assertEquals(true, this.event.get("a1") != event.get("a1"));
		 assertEquals(true, this.event.get("a2") != event.get("a2"));
	     count.incrementAndGet();
	}
	  
  }
  private InboundRESTChannel inboundChannel;
  private OutboundRESTChannel outboundChannel;
  private JetstreamEvent event;
  private InboundChannelSink inboundChannelSink;
  private HttpServer server;

  
  
  public void setUp() throws Exception { 
	
    TestApplicationInformation ai = new TestApplicationInformation(null);
    
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("a1", "v1");
    map.put("a2", "v2");
    event = new JetstreamEvent(map);
    
    new RootConfiguration(ai, new String[] { "src/test/java/com/ebay/jetstream/event/channel/messaging/http/httpmessagingtestwiring.xml" });
    
    inboundChannel = (InboundRESTChannel) RootConfiguration.getConfiguration().getBean("InboundRESTEvents");
    inboundChannelSink = new InboundChannelSink(event);
    
    inboundChannel.addEventSink(inboundChannelSink);

    outboundChannel = (OutboundRESTChannel) RootConfiguration.getConfiguration().getBean("outboundRESTChannel");
    
    server = (HttpServer) RootConfiguration.getConfiguration().getBean("NettyHttpServer");
    
    
  }

 
  @Test
  public void testSendJetstreamEvent() {
	try {
		setUp();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	
	try {
		outboundChannel.sendEvent(event);
	} catch (Throwable t) {
		
	}
    
	int count = 20; 
    while(inboundChannelSink.getCount() == 0) {
    	try {
			Thread.sleep(1000);
			if (count-- == 0) break;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
	
    System.out.println("shutting down outbound channel"); //KEEPME
    outboundChannel.shutDown();
    System.out.println("shutting down inbound channel"); //KEEPME
    inboundChannel.shutDown();
    System.out.println("shutting down Http Server"); //KEEPME
    server.shutDown();
  }

}
