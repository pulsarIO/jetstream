/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.autoflush.handler;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.MessageServiceTimer;

/**
 * @author shmurthy@ebay.com - Timer for flushing the AutoFlushBuffer
 */

public class FlushTimer extends TimerTask {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	
	NettyAutoFlushBatcher m_writer;
	int m_timeIntervalInMillis = 500;

	public FlushTimer(NettyAutoFlushBatcher writer, int intervalInMillis) {

		m_writer = writer;
		m_timeIntervalInMillis = intervalInMillis;
		MessageServiceTimer.sInstance().schedulePeriodicTask(this, m_timeIntervalInMillis);
	}

	@Override
	public void run() {

		try {
			m_writer.flush();
		} catch(Throwable t) {
			LOGGER.error( "Failed to flush - " + t.getLocalizedMessage());
		}

	}

}
