/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

public enum MesssageChannelErrors {

	AFFINITY_KEY_MISSING,
	VIRTUAL_QUEUE_FULL,
	NO_CONSUMERS,
	NO_AFFINITY_CONSUMER,
	CHANNEL_EXCEPTION;
	
}
