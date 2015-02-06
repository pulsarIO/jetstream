/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.ConfigChangeMessage;
import com.ebay.jetstream.config.RootConfiguration;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;


/**
 * Junit Test Case for Messaging
 *
 * @author mvembunarayanan
 *
 */
public class MessageServiceTestApplication {
	
	
  /* Publish */
  class Publish implements Runnable {

    public void run() {
      publishMessage(s_service);
    }
  }

  /* Set up subscribers */
  class Subscribe implements Runnable {

    public void run() {
      initSubscribers(s_service, m_listenerMap);
    }
  }

  private static Integer s_messagesentCount = 0;
  private static volatile Integer s_messagereceivedCount = 0;
  private MessageService s_service;

  public static void setMessageReceivedCount() {
    synchronized (MessageServiceTestApplication.class) {
      ++s_messagereceivedCount;
    }
  }

  List<ContextConfig> contexts = new ArrayList<ContextConfig>();

  private Map<JetstreamTopic, IMessageListener> m_listenerMap;

  public MessageServiceTestApplication() {
    new RootConfiguration(new ApplicationInformation("ConfigChange", "0.0.5.0"),
        new String[] { "src/test/java/com/ebay/jetstream/messaging/MessageContext.xml" });

    s_service = MessageService.getInstance();

    for (TransportConfig te : s_service.getMessageServiceProperties().getTransports())
      for (ContextConfig context : te.getContextList())
        contexts.add(context);

    m_listenerMap = initListeners();
  }

  private Map<JetstreamTopic, IMessageListener> initListeners() {
    Map<JetstreamTopic, IMessageListener> listenerMap = new HashMap<JetstreamTopic, IMessageListener>();

    for (ContextConfig context : contexts)
      listenerMap.put(new JetstreamTopic(context.getContextname()), new MessageServiceTestApplicationListener());

    return listenerMap;
  }

  private void initSubscribers(MessageService service, Map<JetstreamTopic, IMessageListener> map) {

    Iterator<JetstreamTopic> topicIterator = map.keySet().iterator();

    while (topicIterator.hasNext()) {
      JetstreamTopic topic = topicIterator.next();
      try {
        service.subscribe(topic, map.get(topic));
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    
  }

  public void publishMessage(MessageService service) {
    for (ContextConfig context : contexts) {
    	System.out.println("Sending Message for the context :" + context.getContextname()); //KEEPME
    	service.prepareToPublish(new JetstreamTopic(context.getContextname()));
      for (int i = 0; i < MessageSettingParameters.messageCounter; i++) {
        try {
        	
        	service.publish(new JetstreamTopic(context.getContextname()), new ConfigChangeMessage("EFL", "/PGW/sjc-2/dev06.sol001.dev03",
              "version_1.0", "EFL.Cal", ++s_messagesentCount));
        	Thread.sleep(50);
        }
        catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testMessaging() {
	  	  
    /* Subscribe */
    Thread subscribe = new Thread(new Subscribe());
    subscribe.start();

    try {
      subscribe.join();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  
    /* Publish */
    Thread publish = new Thread(new Publish());
    publish.start();

    try {

      publish.join();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }

    /* Give some time for the listeners to receive and process the message before asserting */
    try {
      Thread.sleep(5000);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }

    System.out.println("Message sent = " + s_messagesentCount); //KEEPME
    System.out.println("Message received = " + s_messagereceivedCount); //KEEPME
    assertEquals(true, s_messagesentCount <= s_messagereceivedCount); //KEEPME

    try {
     s_service.shutDown();
    }
    catch (Exception e1) {
      e1.printStackTrace();
    }
    
    try {
		Thread.sleep(1000); 
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
	
  
  }
  
  
    
  public void main(String[] args) throws Exception {
	  
	  MessageServiceTestApplication msta = new MessageServiceTestApplication();
	  
	  msta.testMessaging();
	 
	  
	  
  }
  
/**
 * Set the number of messages to be sent.
 *
 * @author mvembunarayanan
 *
 */
interface MessageSettingParameters {
  int messageCounter = 20;

}

}


