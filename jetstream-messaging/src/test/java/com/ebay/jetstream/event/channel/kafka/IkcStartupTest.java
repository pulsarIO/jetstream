/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static com.ebay.jetstream.event.channel.kafka.KafkaConstants.COMMON_WAIT_INTERVAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;

public class IkcStartupTest extends InboundKafkaChannelTest {

	@Test
	public void testOpenButNotSubscribe() {
		consumerConfig.setSubscribeOnInit(false);
		ikc.open();
		assertEquals(1, ikc.getTotalPauseCount());
	}

	@Test
	public void testOpenAndSubscribe() throws Exception {
		consumerConfig.setSubscribeOnInit(true);
		ikc.open();
		assertEquals(2, getConsumerTaskList().size());
	}

	@Test
	public void testOpenAndSubscribeRetry() throws Exception {
		when(mockController.getZkConnector()).thenReturn(null);
		ikc.setKafkaController(mockController);
		consumerConfig.setSubscribeOnInit(true);
		new Thread() {
			@Override
			public void run() {
				ikc.open();
				int size;
				try {
					size = getConsumerTaskList().size();
					assertEquals(2, size);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}.start();
		Thread.sleep(COMMON_WAIT_INTERVAL + 2000);
		when(mockController.getZkConnector()).thenReturn(zkConnector);
		ikc.setKafkaController(mockController);
	}

	@Test
	public void subscribeWithFixedCountCoordinator() throws Exception {
		consumerConfig.setSubscribeOnInit(true);
		consumerConfig.setDynamicAllocatePartition(false);
		consumerConfig.setFixedPartitionCountPerTopic(1);
		ikc.open();
		assertEquals(2, getConsumerTaskList().size());
		assertTrue(getZkCoordinator() instanceof FixedCountCoordinator);
	}

	@Test
	public void subscribeWithStaticCoordinator() throws Exception {
		consumerConfig.setSubscribeOnInit(true);
		consumerConfig.setDynamicAllocatePartition(false);
		consumerConfig.setFixedPartitionCountPerTopic(0);
		consumerConfig.setAllocatedPartitions("1");
		ikc.open();
		assertEquals(2, getConsumerTaskList().size());
		assertTrue(getZkCoordinator() instanceof StaticCoordinator);
	}

	@Test
	public void testCalcRebalance() {
		// call when not running
		Map<String, Integer> countMap = ikc.calcRebalance();
		assertNull(countMap);

		openIKC();
		countMap = ikc.calcRebalance();
		assertNotNull(countMap);

		// call when paused
		ikc.pause();
		countMap = ikc.calcRebalance();
		assertNull(countMap);
	}

}
