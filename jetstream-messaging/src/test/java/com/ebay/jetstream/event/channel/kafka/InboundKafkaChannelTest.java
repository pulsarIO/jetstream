/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import kafka.common.TopicAndPartition;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.ebay.jetstream.event.channel.kafka.InboundKafkaChannel.ConsumerTask;
import com.ebay.jetstream.event.support.AbstractBatchEventProcessor;

public abstract class InboundKafkaChannelTest {

	protected static TestZookeeperServer zkServer;
	protected static KafkaControllerConfig config;
	protected static String configBeanName = "kafkaControllerConfig";
	protected static KafkaController mockController;
	protected static KafkaController.ZkConnector zkConnector;

	protected static TestKafkaServer kafkaBroker0;
	protected static TestKafkaServer kafkaBroker1;

	protected static TestKafkaMessageSerializer serializer;

	protected static String groupId = "testGroup1";
	protected static String topic = "testTopic";

	protected static int topicIndex = 0;

	protected KafkaConsumerConfig consumerConfig;
	protected InboundKafkaChannel ikc;
	protected AbstractBatchEventProcessor mockBatchEventSink;
	protected KafkaChannelAddress channelAddress;

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

		KafkaController kafkaController = new KafkaController();
		kafkaController.setConfig(config);
		kafkaController.init();
		zkConnector = kafkaController.getZkConnector();

		mockController = mock(KafkaController.class);
		when(mockController.getZkConnector()).thenReturn(zkConnector);

		kafkaBroker0 = new TestKafkaServer("/kafka0/", 9092, 0, zkConnect, 2);
		kafkaBroker1 = new TestKafkaServer("/kafka1/", 9093, 1, zkConnect, 2);

		serializer = new TestKafkaMessageSerializer();
	}

	protected static void addPartition(String topic, int partition, int leaderId) {
		String partitionStatePath = "/brokers/topics/" + topic + "/partitions/"
				+ partition + "/state";
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("leader", leaderId);
		zkConnector.writeJSON(partitionStatePath, map);
	}

	@Before
	public void init() throws Exception {
		consumerConfig = new KafkaConsumerConfig();
		consumerConfig.setEnabled(true);
		consumerConfig.setGroupId(groupId);
		consumerConfig.setPoolSize(2);
		ikc = new TestInboundKafkaChannel();

		mockBatchEventSink = mock(AbstractBatchEventProcessor.class);
		ikc.addBatchEventSink(mockBatchEventSink);
		channelAddress = new KafkaChannelAddress();
		List<String> topicList = new ArrayList<String>();
		topicList.add(topic);
		channelAddress.setChannelTopics(topicList);

		ikc.setBeanName("testIKC");
		ikc.setConfig(consumerConfig);
		ikc.setAddress(channelAddress);
		ikc.setSerializer(serializer);
		ikc.setKafkaController(mockController);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		zkServer.shutdown();
		kafkaBroker0.stop();
		kafkaBroker1.stop();
	}

	protected void openIKC() {
		consumerConfig.setPoolSize(2);
		consumerConfig.setSubscribeOnInit(true);
		consumerConfig.setDynamicAllocatePartition(true);
		ikc.open();
	}

	@SuppressWarnings("unchecked")
	protected List<ConsumerTask> getConsumerTaskList() throws Exception {
		return (List<ConsumerTask>) ReflectFieldUtil.getField(
				InboundKafkaChannel.class, ikc, "m_consumerTasks");
	}

	protected ZkCoordinator getZkCoordinator() throws Exception {
		return (ZkCoordinator) ReflectFieldUtil.getField(
				InboundKafkaChannel.class, ikc, "m_zkCoordinator");
	}

	protected void setZkCoordinator(ZkCoordinator zkCoordinator)
			throws Exception {
		ReflectFieldUtil.setField(InboundKafkaChannel.class, ikc,
				"m_zkCoordinator", zkCoordinator);
	}

	@SuppressWarnings("unchecked")
	protected BlockingQueue<PartitionReader> getPartitionQueue()
			throws Exception {
		return (BlockingQueue<PartitionReader>) ReflectFieldUtil.getField(
				InboundKafkaChannel.class, ikc, "m_queue");
	}

	@SuppressWarnings("unchecked")
	protected Map<TopicAndPartition, PartitionReader> getPartitionMap()
			throws Exception {
		return (Map<TopicAndPartition, PartitionReader>) ReflectFieldUtil
				.getField(InboundKafkaChannel.class, ikc, "m_partitionMap");
	}

	protected PartitionReader getPartitionFromIKC(String topic, int partition)
			throws Exception {
		Map<TopicAndPartition, PartitionReader> map = getPartitionMap();
		TopicAndPartition tp = new TopicAndPartition(topic, partition);
		return map.get(tp);
	}

	protected void invokeTakePartition(String topic, int partition)
			throws Exception {
		Object[] args = new Object[2];
		args[0] = topic;
		args[1] = partition;
		ReflectFieldUtil.invokeMethod2(InboundKafkaChannel.class, ikc,
				"takePartition", args);
	}

}
