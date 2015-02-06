/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventscheduler;

import java.util.Map;

import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing.ConsistentHashing;

public class DefaultRingUpdateListener implements ConsistentHashingRingUpdateListener {

	Map<JetstreamTopic, ConsistentHashing<EventConsumerInfo>> m_chmap;
	
	public DefaultRingUpdateListener() {}
	
	@Override
	public void update(Map<JetstreamTopic, ConsistentHashing<EventConsumerInfo>> chmap, Map<Long, EventConsumerInfo> consumers) {
	
		m_chmap = chmap;
		
	}

}
