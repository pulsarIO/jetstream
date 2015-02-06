/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UncaughtRequestThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.util");

  public void uncaughtException(Thread t, Throwable e) {
      LOGGER.error( "Exception \'" + e.getMessage() + "\' in thread " + t.getId());
  }
}
