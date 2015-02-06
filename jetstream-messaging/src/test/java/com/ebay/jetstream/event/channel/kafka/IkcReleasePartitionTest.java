/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import kafka.common.TopicAndPartition;

import org.junit.Test;

public class IkcReleasePartitionTest extends InboundKafkaChannelTest {

	@Test
	public void testReleasePartitions() throws Exception {
		// call when not running
		boolean rs = ikc.releasePartitions(topic, 1);
		assertFalse(rs);

		// take partition
		openIKC();
		addPartition(topic, 0, 0);
		rs = ikc.takePartitions(topic, 1);
		assertTrue(rs);
		Map<TopicAndPartition, PartitionReader> map = getPartitionMap();
		assertEquals(1, map.size());

		// call when paused
		ikc.pause();
		rs = ikc.releasePartitions(topic, 1);
		assertFalse(rs);
		
		// release OK
		ikc.resume();
		rs = ikc.releasePartitions(topic, 1);
		assertTrue(rs);
		PartitionReader p = map.get(new TopicAndPartition(topic, 0));
		assertNotNull(p);
		assertFalse(p.isTaken());
	}

}
