/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkCoordinatorTest {

	private static TestZookeeperServer zkServer;
	private static KafkaControllerConfig config;
	private static KafkaController kafkaController;
	private static KafkaController.ZkConnector zkConnector;

	private static ZkCoordinator zkCoordinator;

	private static String consumerId = "D-00456-consumerId";
	private static String groupId = "test_group";

	private static String topic1 = "Topic.test-1";
	private static String topic2 = "Topic.test-2";
	private static List<String> topics = new ArrayList<String>();

	static {
		topics.add(topic1);
		topics.add(topic2);
	}

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

		zkCoordinator = new DynamicCoordinator(topics, groupId, consumerId,
				zkConnector);
	}

	@Test
	public void testRegister() {
		zkCoordinator.registerConsumer();

		String groupPath = zkCoordinator.m_groupPath;
		String consumerPath = zkCoordinator.consumerPath(consumerId);

		assertTrue(zkConnector.exists(groupPath));
		assertTrue(zkConnector.exists(consumerPath));
	}

	@Test
	public void testUnregister() {
		zkCoordinator.registerConsumer();
		zkCoordinator.unregisterConsumer();

		String groupPath = zkCoordinator.m_groupPath;
		String consumerPath = zkCoordinator.consumerPath(consumerId);

		assertTrue(zkConnector.exists(groupPath));
		assertFalse(zkConnector.exists(consumerPath));
	}

	@Test
	public void testReregister() {
		String consumerPath = zkCoordinator.consumerPath(consumerId);

		zkCoordinator.reRegisterConsumer(false);
		assertTrue(zkConnector.exists(consumerPath));

		zkCoordinator.reRegisterConsumer(false);
		assertTrue(zkConnector.exists(consumerPath));

		zkCoordinator.reRegisterConsumer(true);
		assertTrue(zkConnector.exists(consumerPath));
	}

	@Test
	public void testTakePartition() {
		String ownerPath = zkCoordinator.ownerPath(topic1, 0);
		zkCoordinator.takePartition(topic1, 0);
		assertTrue(zkConnector.exists(ownerPath));
		String owner = zkConnector.readString(ownerPath);
		assertEquals(consumerId, owner);
	}

	@Test
	public void testResetOwner() {
		String ownerPath = zkCoordinator.ownerPath(topic1, 1);
		zkCoordinator.takePartition(topic1, 1);
		zkCoordinator.resetOwner(topic1, 1);
		assertTrue(zkConnector.exists(ownerPath));
		String owner = zkConnector.readString(ownerPath);
		assertEquals(consumerId, owner);
	}

	@Test
	public void isZkOwnerExist() {
		boolean rs = zkCoordinator.isZkOwnerExist(topic1, 2);
		assertFalse(rs);
		zkCoordinator.takePartition(topic1, 2);
		rs = zkCoordinator.isZkOwnerExist(topic1, 2);
		assertTrue(rs);
	}

	@Test
	public void testReleasePartition() {
		String ownerPath = zkCoordinator.ownerPath(topic1, 3);
		zkCoordinator.takePartition(topic1, 3);
		assertTrue(zkConnector.exists(ownerPath));
		zkCoordinator.releasePartition(topic1, 3);
		assertFalse(zkConnector.exists(ownerPath));
	}

	@Test
	public void testGetMyPartitions() {
		zkCoordinator.takePartition(topic1, 4);
		zkCoordinator.takePartition(topic1, 5);

		Set<Integer> partitions = zkCoordinator.getMyPartitions(topic1);
		assertTrue(partitions.contains(4));
		assertTrue(partitions.contains(5));
	}

	@Test
	public void testGetAllPartitions() {
		Set<Integer> partitions = zkCoordinator.getAllPartitions(topic1);
		assertTrue(partitions.isEmpty());

		String path = zkCoordinator.partitionPath(topic1);
		zkConnector.create(path, false);

		partitions = zkCoordinator.getAllPartitions(topic1);
		assertTrue(partitions.isEmpty());

		zkConnector.create(path + "/6", false);
		zkConnector.create(path + "/7", false);
		zkConnector.create(path + "/8", false);

		partitions = zkCoordinator.getAllPartitions(topic1);
		assertFalse(partitions.isEmpty());
		assertTrue(partitions.contains(6));
		assertTrue(partitions.contains(7));
		assertTrue(partitions.contains(8));
	}

	@Test
	public void testGetRandomReleasePartitions() {
		zkCoordinator.takePartition(topic2, 0);
		zkCoordinator.takePartition(topic2, 1);
		zkCoordinator.takePartition(topic2, 2);

		zkCoordinator.getMyPartitions(topic2);

		Set<Integer> toRelease = zkCoordinator.getRandomReleasePartitions(
				topic2, 2);
		Set<Integer> partitions = new HashSet<Integer>();
		partitions.add(0);
		partitions.add(1);
		partitions.add(2);
		assertEquals(2, toRelease.size());
		assertTrue(partitions.containsAll(toRelease));
	}

	@Test
	public void testGetAllConsumers() throws Exception {
		String consumerId1 = "consumerId1";
		String consumerId2 = "consumerId2";
		DynamicCoordinator consumer1 = new DynamicCoordinator(topics, groupId,
				consumerId1, zkConnector);
		consumer1.registerConsumer();
		DynamicCoordinator consumer2 = new DynamicCoordinator(topics, groupId,
				consumerId2, zkConnector);
		consumer2.registerConsumer();

		zkCoordinator.registerConsumer();

		List<String> consumers = zkCoordinator.getAllConsumers();

		assertTrue(consumers.contains(consumerId1));
		assertTrue(consumers.contains(consumerId2));
		assertTrue(consumers.contains(consumerId));
	}

	@Test
	public void testIsNewPartition() throws Exception {
		String topic11 = "Topic.test-11";
		String path11 = "/brokers/topics/" + topic11 + "/partitions";
		zkConnector.create(path11, false);
		zkConnector.create(path11 + "/0", false);

		String topic12 = "Topic.test-12";
		String path12 = "/brokers/topics/" + topic12 + "/partitions";
		zkConnector.create(path12, false);
		zkConnector.create(path12 + "/0", false);

		List<String> topics = new ArrayList<String>();
		topics.add(topic11);
		topics.add(topic12);

		ZkCoordinator zkCoordinator = new DynamicCoordinator(topics, groupId,
				consumerId, zkConnector);
		
		zkCoordinator.getAllPartitions(topic11);
		zkCoordinator.getAllPartitions(topic12);

		zkConnector.create(path11 + "/1", false);
		zkConnector.create(path12 + "/1", false);

		zkCoordinator.getAllPartitions(topic11);
		zkCoordinator.getAllPartitions(topic12);

		assertTrue(zkCoordinator.isNewPartition(topic11, 1));
		assertTrue(zkCoordinator.isNewPartition(topic12, 1));
		
		
		zkConnector.create(path11 + "/2", false);
		zkConnector.create(path12 + "/2", false);
		
		zkCoordinator.getAllPartitions(topic11);
		zkCoordinator.getAllPartitions(topic12);
		
		assertTrue(zkCoordinator.isNewPartition(topic11, 2));
		assertTrue(zkCoordinator.isNewPartition(topic12, 2));
	}

	@AfterClass
	public static void tearDown() throws Exception {
		kafkaController.shutDown();
		zkServer.shutdown();
	}

}
