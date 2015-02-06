/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.advice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;


public class DefaultAdviceImpl extends AbstractNamedBean implements BeanChangeAware, InitializingBean,
    ApplicationListener, Advice {
	
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.advice");

  public DefaultAdviceImpl() {
    LOGGER.info( "********** Instantiating DefaultAdviceImpl *******************"); //KEEPME
  }

  public void abandon(JetstreamEvent event, int reasonCode, String reason) {
	  LOGGER.info( "********** Abandon Event - " + event.toString() + " --- reason = " + reason); //KEEPME
  }

  public void afterPropertiesSet() throws Exception {
  }

  public void onApplicationEvent(ApplicationEvent event) {
	  LOGGER.info("Spring Container: Received Event: " + event.getClass() + " from: " + event.getSource()); //KEEPME
  }

  public void retry(JetstreamEvent event, RetryEventCode reasonCode, String reason) {
	  LOGGER.info("********** Retry Event - " + event.toString() + " --- reasonCode = " + reasonCode.name()
        + " --- reason = " + reason); //KEEPME
    
   }

  public void success(JetstreamEvent event) {
	  LOGGER.info("********** Event Successfully Processed: " + event.toString() + "**********"); //KEEPME

  }

@Override
public void stopReplay() {
	// TODO Auto-generated method stub
	
}

@Override
public void startReplay() {
	// TODO Auto-generated method stub
	
}
}
