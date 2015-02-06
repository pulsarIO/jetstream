/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

/**
 * @author shmurthy
 * 
 * QueueMonitor - an impl of this interface must be registered with the FifoPriotityQueue
 * to monitor the queue for threshold crossing and get subsequent advise.
 * 
 */
public interface QueueMonitor {

  public void pause(long queueSize, int priority);
  public void resume(long queueSize, int priority);
  public float getHighWaterMark();
  public float getLowWaterMark();
  
}
