/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.messaging.transport.netty.eventscheduler;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.ConsumerChannelContext;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.registry.Registry;

/**
 * @author weifang
 * 
 */
public class WeightedRandomConnectionScheduler extends WeightedRandomScheduler {
	private EventConsumerInfo m_lastConsumer = null;

	@Override
	public EventConsumerInfo scheduleNext(JetstreamMessage msg,
			Registry registry) throws NoConsumerToScheduleException {
		EventConsumerInfo curConsumer = null;
		if (m_lastConsumer != null) {
			ConsumerChannelContext ccc = m_lastConsumer.getNextChannelContext();
			if (ccc != null && ccc.getChannel() != null
					&& ccc.getChannel().isActive()) {
				curConsumer = m_lastConsumer;
			}
		}

		if (curConsumer == null) {
			curConsumer = super.scheduleNext(msg, registry);
			m_lastConsumer = curConsumer;
		}
		return curConsumer;

	}

	public Scheduler clone() throws CloneNotSupportedException {
		WeightedRandomConnectionScheduler newScheduler = new WeightedRandomConnectionScheduler();
		return newScheduler;

	}
}
