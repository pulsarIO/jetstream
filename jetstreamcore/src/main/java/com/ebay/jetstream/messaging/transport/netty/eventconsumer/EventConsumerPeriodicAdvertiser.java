/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventconsumer;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.MessageServiceTimer;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * 
 * Sends out advertisement every 90 secs
 */

public class EventConsumerPeriodicAdvertiser extends TimerTask {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	
	private static final long ONCE_EVERY_NINTY_SECONDS = 90000;

	private EventConsumer m_consumer;

	/**
	 * @param consumer
	 */
	public EventConsumerPeriodicAdvertiser(EventConsumer consumer) {

		m_consumer = consumer;

		MessageServiceTimer.sInstance().schedulePeriodicTask(this,
				ONCE_EVERY_NINTY_SECONDS);

	}

	@Override
	public void run() {

		try {
			m_consumer.advertise(EventConsumer.NOBROADCAST);
		} catch (Throwable t) {
			LOGGER.error( "Failed to advertise - " + t.getLocalizedMessage());
		}
	}

}
