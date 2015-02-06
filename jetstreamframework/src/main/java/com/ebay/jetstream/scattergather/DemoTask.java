/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.scattergather;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoTask implements Task {

  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.scattergather");
  private final int m_taskid;
  private final long m_taskDelay;
  private boolean m_taskStatus;

  public DemoTask(int taskid, long taskDelay) {
    m_taskid = taskid;
    m_taskDelay = taskDelay;

  }

  public boolean execute() {

   
    try {
      Thread.sleep(m_taskDelay);
    }
    catch (InterruptedException e) {
    	
        LOGGER.info( "Sleep interrupted - " + e.getLocalizedMessage());
 
    }
    return false;
  }

  public void executed() {
    m_taskStatus = true;
  }

  public boolean isExecuted() {

    return m_taskStatus;
  }

  public void setAbandonStatusIndicator(AtomicBoolean abandonStatusIndicator) {
    // TODO Auto-generated method stub
    
  }

}
