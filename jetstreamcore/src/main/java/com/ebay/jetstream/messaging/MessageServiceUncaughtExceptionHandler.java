/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shmurthy
 *
 * 
 */
public class MessageServiceUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

    public void uncaughtException(Thread t, Throwable e) {
        LOGGER.error( "Exception \'" + e.getMessage() + "\' in thread " + t.getId());
    }
}
