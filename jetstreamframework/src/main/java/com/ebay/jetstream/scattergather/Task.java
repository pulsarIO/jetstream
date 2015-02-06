/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.scattergather;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * interface for scatter gather task. All implementations of scatter gather tasks must implement this interface
 * 
 * @author shmurthy@ebay.com
 * 
 */
public interface Task {

  public boolean execute();

  public void executed();

  public boolean isExecuted();

  /**
   * @return
   */

  public void setAbandonStatusIndicator(AtomicBoolean abandonStatusIndicator);

}
