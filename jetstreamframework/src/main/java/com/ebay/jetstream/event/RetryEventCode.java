/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.event;

/**
 * @author msubbaiah
 * 
 */
public enum RetryEventCode {

  MSG_RETRY, RES_RETRY, RATEPROCESSOR_OVERFLOW, PAUSE_RETRY, SHUTDOWN, QUEUE_FULL, UNKNOWN;
}
