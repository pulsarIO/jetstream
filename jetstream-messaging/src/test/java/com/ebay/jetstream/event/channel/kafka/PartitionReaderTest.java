/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static kafka.api.OffsetRequest.EarliestTime;
import static kafka.api.OffsetRequest.LatestTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.ebay.jetstream.event.JetstreamEvent;

public class PartitionReaderTest {

	// suppose the size of each event is 1000 bytes
	private static long eachEventInBytes = 1000;

	private static TestZookeeperServer zkServer;
	private static KafkaControllerConfig config;
	private static String configBeanName = "kafkaControllerConfig";
	private static KafkaController kafkaController;
	private static KafkaController.ZkConnector zkConnector;

	private static TestKafkaServer kafkaBroker0;
	private static TestKafkaServer kafkaBroker1;

	private static TestKafkaMessageSerializer serializer;

	private static String groupId = "testGroup1";
	private static String topic = "Topic.test-1";
	private static int partition = 1;

	private static String offsetPath = "/consumers/" + groupId + "/offsets/"
			+ topic + "/" + partition;

	private static KafkaConsumerConfig consumerConfig;

	private static PartitionReader reader;

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

		kafkaBroker0 = new TestKafkaServer("/kafka0/", 9082, 0, zkConnect, 2);
		kafkaBroker1 = new TestKafkaServer("/kafka1/", 9083, 1, zkConnect, 2);

		serializer = new TestKafkaMessageSerializer();

		consumerConfig = new KafkaConsumerConfig();
		consumerConfig.setEnabled(true);
		consumerConfig.setGroupId(groupId);

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

		reader = new PartitionReader(topic, partition, consumerConfig,
				zkConnector, serializer);
	}

	private void mockSimpleConsumerForEarliestOffset(
			SimpleConsumer mockConsumer, String topic, int partition,
			long earliestOffset) {
		TopicAndPartition tp = new TopicAndPartition(topic, partition);
		OffsetResponse earliestResponse = mock(OffsetResponse.class);
		when(earliestResponse.hasError()).thenReturn(false);
		when(earliestResponse.offsets(topic, partition)).thenReturn(
				new long[] { earliestOffset });
		when(
				mockConsumer
						.getOffsetsBefore(argThat(new IsEarliestOffsetRequest(
								tp)))).thenReturn(earliestResponse);
	}

	private void mockSimpleConsumerForLatestOffset(SimpleConsumer mockConsumer,
			String topic, int partition, long latestOffset) {
		TopicAndPartition tp = new TopicAndPartition(topic, partition);
		OffsetResponse latestResponse = mock(OffsetResponse.class);
		when(latestResponse.hasError()).thenReturn(false);
		when(latestResponse.offsets(topic, partition)).thenReturn(
				new long[] { latestOffset });
		when(
				mockConsumer
						.getOffsetsBefore(argThat(new IsLatestOffsetRequest(tp))))
				.thenReturn(latestResponse);
	}

	private void mockSimpleConsumerForOffsetError(SimpleConsumer mockConsumer,
			String topic, int partition) {
		TopicAndPartition tp = new TopicAndPartition(topic, partition);
		OffsetResponse offsetResponse = mock(OffsetResponse.class);
		when(offsetResponse.hasError()).thenReturn(true);
		when(offsetResponse.errorCode(topic, partition)).thenReturn(
				ErrorMapping.UnknownTopicOrPartitionCode());
		when(mockConsumer.getOffsetsBefore(argThat(new IsOffsetRequest())))
				.thenReturn(offsetResponse);
	}

	private void mockSimpleConsumerForOffsetOutOfRange(
			SimpleConsumer mockConsumer, String topic, int partition) {
		FetchResponse fetchResponse = mock(FetchResponse.class);
		when(fetchResponse.hasError()).thenReturn(true);
		when(fetchResponse.errorCode(topic, partition)).thenReturn(
				ErrorMapping.OffsetOutOfRangeCode());
		when(mockConsumer.fetch(argThat(new IsFetchRequest()))).thenReturn(
				fetchResponse);
	}

	@SuppressWarnings("unchecked")
	private void mockSimpleConsumerForFetchException(SimpleConsumer mockConsumer) {
		when(mockConsumer.fetch(argThat(new IsFetchRequest()))).thenThrow(
				FetchFailException.class);
	}

	private void mockSimpleConsumerForNull(SimpleConsumer mockConsumer) {
		when(mockConsumer.fetch(argThat(new IsFetchRequest())))
				.thenReturn(null);
	}

	private void mockSimpleConsumerForRead(SimpleConsumer mockConsumer,
			String topic, int partition, long readOffset, long readSizeInBytes) {
		List<MessageAndOffset> list = new ArrayList<MessageAndOffset>();
		for (int i = 0; i < readSizeInBytes / eachEventInBytes; i++) {
			JetstreamEvent event = new JetstreamEvent();
			byte[] key = serializer.encodeMessage(event);
			byte[] payload = serializer.encodeMessage(event);
			Message msg = mock(Message.class);
			when(msg.key()).thenReturn(ByteBuffer.wrap(key));
			when(msg.payload()).thenReturn(ByteBuffer.wrap(payload));
			MessageAndOffset msgOffset = new MessageAndOffset(msg, readOffset
					+ i);
			list.add(msgOffset);
		}
		ByteBufferMessageSet messageSet = mock(ByteBufferMessageSet.class);
		when(messageSet.iterator()).thenReturn(list.iterator());
		FetchResponse fetchResponse = mock(FetchResponse.class);
		when(fetchResponse.hasError()).thenReturn(false);
		when(fetchResponse.messageSet(topic, partition)).thenReturn(messageSet);
		when(mockConsumer.fetch(argThat(new IsFetchRequest()))).thenReturn(
				fetchResponse);
	}

	@Test
	public void testCompare() {
		PartitionReader reader0 = new PartitionReader(topic, 0, consumerConfig,
				zkConnector, serializer);
		PartitionReader reader1 = new PartitionReader(topic, 1, consumerConfig,
				zkConnector, serializer);
		reader0.setNextFetchInMs(10000);
		reader1.setNextFetchInMs(20000);
		assertTrue(reader0.compareTo(reader1) < 0);
		assertTrue(reader1.compareTo(reader0) > 0);
		reader1.setNextFetchInMs(10000);
		assertTrue(reader1.compareTo(reader0) == 0);
	}

	@Test
	public void testCalWaitTime() {
		PartitionReader reader0 = new PartitionReader(topic, 0, consumerConfig,
				zkConnector, serializer);
		reader0.setNextFetchInMs(System.currentTimeMillis() - 1000);
		assertTrue(reader0.calcWaitTime() < 0);
		reader0.setNextFetchInMs(System.currentTimeMillis() + 1000);
		assertTrue(reader0.calcWaitTime() > 0);
	}

	@Test
	public void testReadEvents() throws Exception {
		int expectedCount = (int) (consumerConfig.getBatchSizeBytes() / eachEventInBytes);
		long readOffset = 1250;
		reader.setReadOffset(readOffset);
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForRead(mockConsumer, topic, partition, readOffset,
				consumerConfig.getBatchSizeBytes());
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);
		reader.setNextBatchSizeBytes(-100);

		List<JetstreamEvent> events = reader.readEvents();
		assertEquals(expectedCount, events.size());
		assertEquals(readOffset + expectedCount, reader.getReadOffset());
	}

	@Test
	public void testInitOffset() throws Exception {
		long smallest = 100, largest = 10000;

		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForEarliestOffset(mockConsumer, topic, partition,
				smallest);
		mockSimpleConsumerForLatestOffset(mockConsumer, topic, partition,
				largest);

		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);

		// if offset not in zookeeper, and init with largest
		zkConnector.delete(offsetPath);
		reader.setReadOffset(-1);
		consumerConfig.setAutoOffsetReset("largest");
		reader.initOffset(false);
		long readOffset = reader.getReadOffset();
		assertEquals(largest, readOffset);

		// if offset exist in zookeeper, init with it
		zkConnector.writeString(offsetPath, String.valueOf(2000));
		reader.setReadOffset(-1);
		consumerConfig.setAutoOffsetReset("largest");
		reader.initOffset(false);
		readOffset = reader.getReadOffset();
		assertEquals(2000, readOffset);

		// if offset not in zookeeper, and init with smallest
		zkConnector.delete(offsetPath);
		reader.setReadOffset(-1);
		consumerConfig.setAutoOffsetReset("smallest");
		reader.initOffset(false);
		readOffset = reader.getReadOffset();
		assertEquals(smallest, readOffset);

		// if a new partition, should always init with smallest although set
		// largest
		zkConnector.delete(offsetPath);
		reader.setReadOffset(-1);
		consumerConfig.setAutoOffsetReset("largest");
		reader.initOffset(true);
		readOffset = reader.getReadOffset();
		assertEquals(smallest, readOffset);

		// if offset exists, but data illegal
		zkConnector.writeString(offsetPath, "offset");
		boolean thrown = false;
		try {
			reader.initOffset(false);
		} catch (Exception e) {
			assertTrue(e instanceof RuntimeException);
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void testOffsetError() throws Exception {
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForOffsetError(mockConsumer, topic, partition);
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);
		try {
			reader.resetOffset();
		} catch (Exception e) {
		}
		verify(mockConsumer, atLeast(2)).getOffsetsBefore(
				argThat(new IsOffsetRequest()));
	}

	@Test
	public void testResetOffset() throws Exception {
		long smallest = 100, largest = 10000;
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForEarliestOffset(mockConsumer, topic, partition,
				smallest);
		mockSimpleConsumerForLatestOffset(mockConsumer, topic, partition,
				largest);
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);

		reader.resetOffset();
		long readOffset = reader.getReadOffset();
		assertEquals(largest, readOffset);

		consumerConfig.setAutoOffsetReset("smallest");
		reader.resetOffset();
		readOffset = reader.getReadOffset();
		assertEquals(smallest, readOffset);
	}

	@Test
	public void testRevertOffset() throws Exception {
		long smallest = 100, largest = 10000;
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForEarliestOffset(mockConsumer, topic, partition,
				smallest);
		mockSimpleConsumerForLatestOffset(mockConsumer, topic, partition,
				largest);
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);

		zkConnector.create(offsetPath, false);
		zkConnector.writeString(offsetPath, String.valueOf(2000));
		reader.setReadOffset(3000);
		assertEquals(3000, reader.getReadOffset());

		reader.revertOffset();
		assertEquals(2000, reader.getReadOffset());

		// if illegal offset in zookeeper
		zkConnector.writeString(offsetPath, String.valueOf(-2));
		reader.setReadOffset(3000);
		assertEquals(3000, reader.getReadOffset());
		consumerConfig.setAutoOffsetReset("largest");
		reader.revertOffset();
		assertEquals(largest, reader.getReadOffset());
	}

	@Test
	public void testCommitOffset() {
		zkConnector.delete(offsetPath);
		reader.setReadOffset(12345);
		reader.commitOffset();
		reader.setReadOffset(0);
		reader.revertOffset();
		assertEquals(12345, reader.getReadOffset());
	}

	@Test
	public void testSetToRelease() {
		reader.setToRelease();
		assertFalse(reader.isTaken());
	}

	@Test
	public void testSetToLost() {
		reader.setLost();
		assertTrue(reader.isLost());
	}

	@Test
	public void testIdle() throws Exception {
		reader.fistIdle();
		consumerConfig.setIdleTimeInMs(3000);
		assertTrue(reader.isIdled());
		assertTrue(reader.getIdleTime() <= consumerConfig.getIdleTimeInMs());
		Thread.sleep(4000);
		assertTrue(reader.getIdleTime() >= consumerConfig.getIdleTimeInMs());
		reader.setOnIdled();
		assertTrue(reader.isOnIdled());

		reader.fistIdle();
		assertTrue(reader.isIdled());
		reader.clearIdleStats();
		assertFalse(reader.isIdled());
	}

	@Test
	public void testSetNextBatchSize() throws Exception {
		int nextBatchSizeBytes = (Integer) ReflectFieldUtil.getField(reader,
				"m_nextBatchSizeBytes");
		assertEquals(consumerConfig.getBatchSizeBytes(), nextBatchSizeBytes);

		reader.setNextBatchSizeBytes(-100);
		nextBatchSizeBytes = (Integer) ReflectFieldUtil.getField(reader,
				"m_nextBatchSizeBytes");
		assertEquals(consumerConfig.getBatchSizeBytes(), nextBatchSizeBytes);

		reader.setNextBatchSizeBytes(10000);
		nextBatchSizeBytes = (Integer) ReflectFieldUtil.getField(reader,
				"m_nextBatchSizeBytes");
		assertTrue(consumerConfig.getBatchSizeBytes() != nextBatchSizeBytes);
		assertEquals(10000, nextBatchSizeBytes);
	}

	@Test
	public void testGetTopic() {
		assertEquals(topic, reader.getTopic());
	}

	@Test
	public void testGetPartition() {
		assertEquals(partition, reader.getPartition());
	}

	@Test
	public void testSkipReadEvents() throws Exception {
		reader.setNextBatchSizeBytes(0);
		List<JetstreamEvent> events = reader.readEvents();
		assertTrue(events.isEmpty());
		int nextBatchSizeBytes = (Integer) ReflectFieldUtil.getField(reader,
				"m_nextBatchSizeBytes");
		assertEquals(consumerConfig.getBatchSizeBytes(), nextBatchSizeBytes);
	}

	@Test
	public void testReadOffsetOutOfRange() throws Exception {
		long smallest = 100, largest = 10000;
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForEarliestOffset(mockConsumer, topic, partition,
				smallest);
		mockSimpleConsumerForLatestOffset(mockConsumer, topic, partition,
				largest);
		mockSimpleConsumerForOffsetOutOfRange(mockConsumer, topic, partition);
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);

		boolean thrown = false;
		try {
			reader.readEvents();
		} catch (Exception e) {
			assertTrue(e instanceof OffsetOutOfRangeException);
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void testFetchException() throws Exception {
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForFetchException(mockConsumer);
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);
		boolean thrown = false;
		try {
			reader.readEvents();
		} catch (Exception e) {
			assertTrue(e instanceof RuntimeException);
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void testReadEventsAndSkipSomeOldOnes() throws Exception {
		int expectedCount = (int) (consumerConfig.getBatchSizeBytes() / eachEventInBytes);
		long readOffset = 1250;
		reader.setReadOffset(readOffset);
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForRead(mockConsumer, topic, partition,
				readOffset - 100, consumerConfig.getBatchSizeBytes());
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);
		reader.setNextBatchSizeBytes(-100);

		List<JetstreamEvent> events = reader.readEvents();
		assertEquals(expectedCount - 100, events.size());
		assertEquals(readOffset + expectedCount - 100, reader.getReadOffset());
	}

	@Test
	public void testReadEventsAndFetchNull() throws Exception {
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		this.mockSimpleConsumerForNull(mockConsumer);
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);

		try {
			List<JetstreamEvent> events = reader.readEvents();
		} catch (Exception e) {
		}
		verify(mockConsumer, atLeast(2)).fetch(argThat(new IsFetchRequest()));
	}

	@Test
	public void testReadEventsAndBlocked() throws Exception {
		int expectedCount = (int) (consumerConfig.getBatchSizeBytes() / eachEventInBytes);
		long readOffset = 1250;
		reader.setReadOffset(readOffset);
		// cannot read any events although not to the end
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		mockSimpleConsumerForRead(mockConsumer, topic, partition, readOffset, 0);
		long largest = 1000000000;
		mockSimpleConsumerForLatestOffset(mockConsumer, topic, partition,
				largest);
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);
		reader.setNextBatchSizeBytes(-100);

		try {
			List<JetstreamEvent> events = reader.readEvents();
		} catch (Exception e) {
		}
		verify(mockConsumer, atLeast(2)).fetch(argThat(new IsFetchRequest()));
	}

	@Test
	public void testClose() throws Exception {
		SimpleConsumer mockConsumer = mock(SimpleConsumer.class);
		ReflectFieldUtil.setField(reader, "m_consumer", mockConsumer);
		reader.close();
		verify(mockConsumer).close();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		kafkaController.shutDown();
		zkServer.shutdown();
		kafkaBroker0.stop();
		kafkaBroker1.stop();
	}

	class IsOffsetRequest extends ArgumentMatcher<OffsetRequest> {

		@Override
		public boolean matches(Object argument) {
			return (argument instanceof OffsetRequest);
		}

	}

	class IsEarliestOffsetRequest extends ArgumentMatcher<OffsetRequest> {

		private TopicAndPartition tp;

		public IsEarliestOffsetRequest(TopicAndPartition tp) {
			this.tp = tp;
		}

		@Override
		public boolean matches(Object argument) {
			if (!(argument instanceof OffsetRequest))
				return false;
			OffsetRequest req = (OffsetRequest) argument;
			PartitionOffsetRequestInfo reqInfo = req.underlying().requestInfo()
					.get(tp).get();
			if (reqInfo.time() == EarliestTime())
				return true;
			return false;
		}

	}

	class IsLatestOffsetRequest extends ArgumentMatcher<OffsetRequest> {

		private TopicAndPartition tp;

		public IsLatestOffsetRequest(TopicAndPartition tp) {
			this.tp = tp;
		}

		@Override
		public boolean matches(Object argument) {
			if (!(argument instanceof OffsetRequest))
				return false;
			OffsetRequest req = (OffsetRequest) argument;
			PartitionOffsetRequestInfo reqInfo = req.underlying().requestInfo()
					.get(tp).get();
			if (reqInfo.time() == LatestTime())
				return true;
			return false;
		}

	}

	class IsFetchRequest extends ArgumentMatcher<FetchRequest> {

		@Override
		public boolean matches(Object argument) {
			return (argument instanceof FetchRequest);
		}

	}

	class FetchFailException extends Exception {

		private static final long serialVersionUID = 7394596392413986570L;

	}

}
