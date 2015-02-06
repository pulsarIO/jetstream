/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.event.support;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NamedBean;
import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.event.channel.InboundChannel;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author msubbaiah
 * 
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={"ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})

public class ShutDownOrchestrator implements InitializingBean, NamedBean, BeanNameAware, XSerializable {
  @Hidden
  public static ShutDownOrchestrator getInstance() {
    return s_shutDownOrchestrator;
  }

  String beanName;

  private List<ShutDownable> m_shutDownComponent; 

  private static ShutDownOrchestrator s_shutDownOrchestrator;
  
  private int waitingTime = 5000;
  
  Logger logger = LoggerFactory.getLogger("com.ebay.jetstream.event.support.ShutDownOrchestrator");
	

  /**
   * 
   * @param shutDown
   */
  public void addComponent(ShutDownable shutDown) {
    if (m_shutDownComponent == null) {
      m_shutDownComponent = new ArrayList<ShutDownable>();
    }
    m_shutDownComponent.add(shutDown);
  }

  public void afterPropertiesSet() throws Exception {
    s_shutDownOrchestrator = this; // this is done for Spring.
    Management.addBean(getBeanName(), this);
  }

  public String getBeanName() {
    return beanName;
  }

  public List<ShutDownable> getShutDownComponent() {
    return m_shutDownComponent;
  }

  public int getWaitingTime() {
    return waitingTime;
  }

  public void setBeanName(String name) {
    beanName = name;
  }

  public void setShutDownComponent(List<ShutDownable> shutDownComponent) {
    m_shutDownComponent = shutDownComponent;
  }

  public void setWaitingTime(int waitingTime) {
    this.waitingTime = waitingTime;
  }
  
  private void pauseAllInboundChannels() {
	  
	  for (ShutDownable component : m_shutDownComponent) {
		  if (component instanceof InboundChannel) {
			  try {
				  ((InboundChannel) component).pause();
			  } catch (Throwable t) {
				  logger.error( t.getLocalizedMessage(), t);
			  }
		  }
	  }
		  
  }

  /**
   * @throws Exception
   * 
   */

  @ManagedOperation
  public void shutDown() {
    try {
      if (m_shutDownComponent != null) {
    	    	  
    	// now issue shutdown to all components in the order specified.
    	// graceful shutdown involves processing all events in queues before returning from shutdown call
    	
       
        for (ShutDownable component : m_shutDownComponent) {
          try {
            component.shutDown();
            logger.warn( component.toString() + " has drained ");
            
          }
          catch (Throwable t) {
            logger.error( t.getLocalizedMessage(), t);
          }
        }
        logger.warn( "Shutdown operation has completed.. ");
        Thread.sleep(waitingTime);
        
        // now check how many components have unprocessed events and report it
        
        int eventCount = 0;
        for (ShutDownable component : m_shutDownComponent) {
          eventCount = eventCount + component.getPendingEvents();
          logger.warn( component.toString() + " has  " + component.getPendingEvents()
              + " events to drain", "ShutDown");
          
        }
        
        
        if (eventCount > 0) {
          logger.warn( "System has  totally " + eventCount
              + " events in its internal queue. It will take some time to shutdown...........Please wait........");
              
          
        }
      }

    }
    catch (Exception e) {
      logger.error( e.getLocalizedMessage(), e);
    }
  }
}
