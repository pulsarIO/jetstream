/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PartitionReaderInitTest {

	private static TestZookeeperServer zkServer;
	private static KafkaControllerConfig config;
	private static String configBeanName = "kafkaControllerConfig";
	private static KafkaController kafkaController;
	private static KafkaController.ZkConnector zkConnector;

	private static TestKafkaServer kafkaBroker0;
	private static TestKafkaServer kafkaBroker1;

	private static TestKafkaMessageSerializer serializer;

	private static TestKafkaProducer producer;

	private static String groupId = "testGroup1";
	private static String topic = "Topic.test-1";
	private static KafkaConsumerConfig consumerConfig;

	@BeforeClass
	public static void setUp() throws Exception {
		try {
			zkServer = new TestZookeeperServer(30000, 2183, 100);
			zkServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		String zkConnect = "localhost:2183";
		config = new KafkaControllerConfig();
		config.setBeanName(configBeanName);
		config.setRebalanceInterval(3000);
		config.setRebalanceableWaitInMs(0);
		config.setZkConnect(zkConnect);

		kafkaController = new KafkaController();
		kafkaController.setConfig(config);

		kafkaController.init();
		zkConnector = kafkaController.getZkConnector();

		kafkaBroker0 = new TestKafkaServer("/kafka0/", 9092, 0, zkConnect, 2);
		kafkaBroker1 = new TestKafkaServer("/kafka1/", 9093, 1, zkConnect, 2);

		serializer = new TestKafkaMessageSerializer();

		producer = new TestKafkaProducer(topic,
				"localhost:9092,localhost:9093", serializer);

		consumerConfig = new KafkaConsumerConfig();
		consumerConfig.setEnabled(true);
		consumerConfig.setGroupId(groupId);
	}

	@Test
	public void testInitFail() {
		try {
			PartitionReader reader = new PartitionReader(topic, 0,
					consumerConfig, zkConnector, serializer);
		} catch (Exception e) {
			assertTrue(e instanceof RuntimeException);
		}
	}

	@Test
	public void testInit() {
		String partitionStatePath0 = "/brokers/topics/" + topic
				+ "/partitions/" + 0 + "/state";
		Map<String, Object> map0 = new HashMap<String, Object>();
		map0.put("leader", 0);
		zkConnector.writeJSON(partitionStatePath0, map0);

		String partitionStatePath1 = "/brokers/topics/" + topic
				+ "/partitions/" + 1 + "/state";
		Map<String, Object> map1 = new HashMap<String, Object>();
		map1.put("leader", 1);
		zkConnector.writeJSON(partitionStatePath1, map1);

		PartitionReader reader = new PartitionReader(topic, 0, consumerConfig,
				zkConnector, serializer);

		assertTrue(reader.isTaken());

	}

	@AfterClass
	public static void tearDown() throws Exception {
		producer.close();
		kafkaController.shutDown();
		zkServer.shutdown();
		kafkaBroker0.stop();
		kafkaBroker1.stop();
	}

}
