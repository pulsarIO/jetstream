/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.BeanChangeInformation;
import com.ebay.jetstream.config.Configuration;
import com.ebay.jetstream.config.ContextBeanChangingEvent;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

/**
 * Listener for Mongo configuration changes done by TOPS. It changes the bean definition and calls the listener
 * registered for this application and the application should change the reference depending on its usecase.
 * 
 */
public class MongoConfigChangeListener implements IMessageListener {
  private final Configuration m_configuration;
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoConfigChangeListener.class.getPackage().getName());
  private ApplicationInformation m_appInfo;

  public MongoConfigChangeListener(Configuration configuration, ApplicationInformation appInfo) {
    m_configuration = configuration;
    m_appInfo = appInfo;
  }

  public void onMessage(JetstreamMessage message) {
    if (message instanceof BeanChangeInformation) {
      BeanChangeInformation beanChangeInfo = (BeanChangeInformation) message;
      
      if (LOGGER.isInfoEnabled()) {
        LOGGER.warn( "onMessage method called");
        LOGGER.warn( "Application : " + beanChangeInfo.getApplication());
        LOGGER.warn( "Scope : " + beanChangeInfo.getScope());
        LOGGER.warn( "Version : " + beanChangeInfo.getVersionString());
        LOGGER.warn( "Bean Name : " + beanChangeInfo.getBeanName());
      }
      
      if ((beanChangeInfo.getApplication().equals(m_appInfo.getApplicationName())) && (beanChangeInfo.getVersionString().equals(m_appInfo.getConfigVersion())))
      {
    	  m_configuration.publishEvent(new ContextBeanChangingEvent(m_configuration, beanChangeInfo));
    	  LOGGER.warn( "Publish bean Changing event for Bean  : " + beanChangeInfo.getBeanName());
      }
    }
  }
}
