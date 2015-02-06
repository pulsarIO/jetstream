/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.messaging.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextStoppedEvent;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;


/**
 * 
 * 
 * Support MessageService code-less configuration via spring.
 */
public class MessageServiceConfiguration implements ApplicationListener, BeanChangeAware, ShutDownable {

  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

  /**
   * @return
   */
  public MessageServiceProperties getMessageServiceProperties() {
    return MessageService.getInstance().getMessageServiceProperties();
  }

  /* (non-Javadoc)
   * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
   */
  public void onApplicationEvent(ApplicationEvent event) {
    if (event instanceof ContextClosedEvent || event instanceof ContextStoppedEvent)
      try {
        MessageService.getInstance().shutDown();
      }
    catch (Throwable e) {
      LOGGER.error( e.toString());
    }

    if (event instanceof ContextBeanChangedEvent) {

      ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

      // Calculate changes
      if (bcInfo.isChangedBean(getMessageServiceProperties())) {

        LOGGER.info( "Received new configuration for  - " + bcInfo.getBeanName());

        try {
          setMessageServiceProperties((MessageServiceProperties) bcInfo.getChangedBean());
        }
        catch (Exception e) {
          LOGGER.error( "Error while applying config to Message Service - " + e.getMessage());
        }
      }
    }
  }

  /**
   * @param msp
   * @throws Exception
   */
  public void setMessageServiceProperties(MessageServiceProperties msp) throws Exception {
    MessageService.getInstance().setMessageServiceProperties(msp);
  }

@Override
public int getPendingEvents() {
	return 0;
}

@Override
public void shutDown() {
	try {
        MessageService.getInstance().shutDown();
      }
    catch (Throwable e) {
      LOGGER.error( e.toString());
    }

	
}
}
