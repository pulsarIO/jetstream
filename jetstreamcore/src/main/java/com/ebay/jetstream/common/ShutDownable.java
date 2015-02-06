/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.common;

/**
 * @author shmurthy@ebay.com
 * 
 */
public interface ShutDownable {

  public int getPendingEvents();

  public void shutDown();

}
