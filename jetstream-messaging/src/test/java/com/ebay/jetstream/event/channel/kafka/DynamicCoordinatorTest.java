/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DynamicCoordinatorTest {

	private static TestZookeeperServer zkServer;
	private static KafkaControllerConfig config;
	private static KafkaController kafkaController;
	private static KafkaController.ZkConnector zkConnector;

	@BeforeClass
	public static void setUp() throws Exception {
		try {
			zkServer = new TestZookeeperServer(30000, 2183, 100);
			zkServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		config = new KafkaControllerConfig();
		config.setRebalanceInterval(3000);
		config.setRebalanceableWaitInMs(0);
		config.setZkConnect("localhost:2183");

		kafkaController = new KafkaController();
		kafkaController.setConfig(config);

		kafkaController.init();
		zkConnector = kafkaController.getZkConnector();
	}

	@Test
	public void testGetRebalanceCount() throws Exception {
		String groupId1 = "test_group1";
		String topic0 = "Topic.test-0";
		String topic1 = "Topic.test-1";
		List<String> topics1 = new ArrayList<String>();
		topics1.add(topic0);
		topics1.add(topic1);

		String consumerId0 = "D-00456-consumerId-0";
		String consumerId1 = "D-00456-consumerId-1";

		ZkCoordinator zkCoordinator0 = new DynamicCoordinator(topics1,
				groupId1, consumerId0, zkConnector);
		ZkCoordinator zkCoordinator1 = new DynamicCoordinator(topics1,
				groupId1, consumerId1, zkConnector);

		try {
			zkCoordinator0.getRebalanceCount();
		} catch (Exception e) {
			assertTrue(e instanceof RuntimeException);
		}

		zkCoordinator0.registerConsumer();

		Map<String, Integer> topicCount0 = zkCoordinator0.getRebalanceCount();
		assertTrue(topicCount0.get(topic0) == 0);
		assertTrue(topicCount0.get(topic1) == 0);

		String path0 = zkCoordinator0.partitionPath(topic0);
		zkConnector.create(path0, false);
		zkConnector.create(path0 + "/0", false);
		zkConnector.create(path0 + "/1", false);
		zkConnector.create(path0 + "/2", false);

		topicCount0 = zkCoordinator0.getRebalanceCount();
		assertTrue(topicCount0.get(topic0) == 3);
		assertTrue(topicCount0.get(topic1) == 0);

		String path1 = zkCoordinator0.partitionPath(topic1);
		zkConnector.create(path1, false);
		zkConnector.create(path1 + "/0", false);
		zkConnector.create(path1 + "/1", false);

		zkCoordinator1.registerConsumer();

		topicCount0 = zkCoordinator0.getRebalanceCount();
		Map<String, Integer> topicCount1 = zkCoordinator1.getRebalanceCount();
		int topic0TakenByC0 = topicCount0.get(topic0);
		int topic1TakenByC0 = topicCount0.get(topic1);
		int topic0TakenByC1 = topicCount1.get(topic0);
		int topic1TakenByC1 = topicCount1.get(topic1);

		assertTrue(topic0TakenByC0 + topic0TakenByC1 == 3);
		assertTrue(Math.abs(topic0TakenByC0 - topic0TakenByC1) <= 1);
		assertTrue(topic1TakenByC0 == 1);
		assertTrue(topic1TakenByC1 == 1);
	}

	@Test
	public void testGetToTakePartitions() throws Exception {
		String groupId2 = "test_group2";
		String topic2 = "Topic.test-2";
		List<String> topics2 = new ArrayList<String>();
		topics2.add(topic2);
		String consumerId2 = "D-00456-consumerId-2";
		ZkCoordinator zkCoordinator2 = new DynamicCoordinator(topics2,
				groupId2, consumerId2, zkConnector);
		
		Set<Integer> toTake = zkCoordinator2.getToTakePartitions(topic2);
		assertTrue(toTake.isEmpty());
		
		String path2 = zkCoordinator2.partitionPath(topic2);
		zkConnector.create(path2, false);
		zkConnector.create(path2 + "/0", false);
		zkConnector.create(path2 + "/1", false);
		
		toTake = zkCoordinator2.getToTakePartitions(topic2);
		assertEquals(2, toTake.size());
		assertTrue(toTake.contains(0));
		assertTrue(toTake.contains(1));
		
		zkCoordinator2.takePartition(topic2, 0);
		toTake = zkCoordinator2.getToTakePartitions(topic2);
		assertEquals(1, toTake.size());
		assertTrue(toTake.contains(1));
	}

	@Test
	public void testGetToReleasePartitions() throws Exception {
		String groupId3 = "test_group3";
		String topic3 = "Topic.test-3";
		List<String> topics3 = new ArrayList<String>();
		topics3.add(topic3);
		String consumerId3 = "D-00456-consumerId-3";
		ZkCoordinator zkCoordinator3 = new DynamicCoordinator(topics3,
				groupId3, consumerId3, zkConnector);
		
		zkCoordinator3.takePartition(topic3, 0);
		zkCoordinator3.takePartition(topic3, 1);
		zkCoordinator3.takePartition(topic3, 2);
		
		Set<Integer> toRelease = zkCoordinator3.getToReleasePartitions(topic3, 2);
		assertEquals(2, toRelease.size());
		Set<Integer> partitions = new HashSet<Integer>();
		partitions.add(0);
		partitions.add(1);
		partitions.add(2);
		assertTrue(partitions.containsAll(toRelease));
	}

	@AfterClass
	public static void tearDown() throws Exception {
		kafkaController.shutDown();
		zkServer.shutdown();
	}

}
