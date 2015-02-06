/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.interfaces;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

/**
 * 
 * 
 * @author shmurthy (shmurthy@ebay.com)
 * 
 * Interface that must be implemented by all listener implementations
 * registered with MessageService to subscribe with MessageService
 * to receive messages 
 */

public interface IMessageListener {

	/**
	 * @param m
	 */
	public void onMessage(JetstreamMessage m);

}