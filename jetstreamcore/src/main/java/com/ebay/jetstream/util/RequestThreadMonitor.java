/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author shmurthy
 * 
 */
public class RequestThreadMonitor {

  private final AtomicLong m_ActiveThreadCount = new AtomicLong(0);

  /**
   * @return
   */
  public long getActiveThreadCount() {
    return m_ActiveThreadCount.get();
  }

  /**
   * 
   */
  public void requestExecuted() {
    m_ActiveThreadCount.decrementAndGet();
  }

  /**
   * 
   */
  public void requestSubmitted() {
    m_ActiveThreadCount.incrementAndGet();
  }
}
