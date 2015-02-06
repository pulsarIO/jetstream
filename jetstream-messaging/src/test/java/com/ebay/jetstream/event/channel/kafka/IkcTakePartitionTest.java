/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Map;

import kafka.common.TopicAndPartition;

import org.junit.Test;

public class IkcTakePartitionTest extends InboundKafkaChannelTest {

	@Test
	public void testTakePartitions() throws Exception {
		addPartition(topic, 0, 0);
		addPartition(topic, 1, 1);

		// call when not running
		boolean rs = ikc.takePartitions(topic, 1);
		assertFalse(rs);

		// take partitions normally
		openIKC();
		rs = ikc.takePartitions(topic, 2);
		assertTrue(rs);
		TopicAndPartition tp0 = new TopicAndPartition(topic, 0);
		TopicAndPartition tp1 = new TopicAndPartition(topic, 1);
		Map<TopicAndPartition, PartitionReader> map = getPartitionMap();
		assertEquals(2, map.size());
		assertTrue(map.containsKey(tp0) && map.containsKey(tp1));

		// take partitions that have been taken
		invokeTakePartition(topic, 0);
		assertEquals(2, map.size());

		// call when paused
		ikc.pause();
		rs = ikc.takePartitions(topic, 1);
		assertFalse(rs);
	}

	@Test
	public void testTakePartitionFail() throws Exception {
		openIKC();
		ZkCoordinator zkCoordinator = getZkCoordinator();
		ZkCoordinator mockCoordinator = spy(zkCoordinator);
		when(mockCoordinator.takePartition(anyString(), anyInt())).thenReturn(
				false);
		setZkCoordinator(mockCoordinator);

		addPartition(topic, 11, 0);

		boolean rs = ikc.takePartitions(topic, 1);
		assertFalse(rs);

		Map<TopicAndPartition, PartitionReader> map = getPartitionMap();
		assertTrue(map.isEmpty());
	}

	@Test
	public void testTakePartitionException() throws Exception {
		openIKC();

		ZkCoordinator zkCoordinator = getZkCoordinator();
		ZkCoordinator mockCoordinator = spy(zkCoordinator);
		when(mockCoordinator.takePartition(anyString(), anyInt())).thenThrow(
				new RuntimeException());
		setZkCoordinator(mockCoordinator);

		addPartition(topic, 21, 0);

		boolean rs = ikc.takePartitions(topic, 1);
		assertFalse(rs);

		Map<TopicAndPartition, PartitionReader> map = getPartitionMap();
		assertTrue(map.isEmpty());
	}

}
