/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventscheduler;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * 
 * 
 */
public class NoConsumerToScheduleException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param message
	 */
	public NoConsumerToScheduleException(String message) {
		super(message);
	}
}
