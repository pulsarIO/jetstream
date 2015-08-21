/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static kafka.api.OffsetRequest.CurrentVersion;
import static kafka.api.OffsetRequest.EarliestTime;
import static kafka.api.OffsetRequest.LargestTimeString;
import static kafka.api.OffsetRequest.LatestTime;
import static kafka.api.OffsetRequest.SmallestTimeString;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;

/**
 * @author xiaojuwu1
 * 
 */
public class PartitionReader implements Comparable<PartitionReader> {

	private static final Logger LOGGER = LoggerFactory.getLogger(PartitionReader.class
			.getName());

	private final static String kmKey = JetstreamReservedKeys.EventKafkaMeta
			.toString();

	private final KafkaController.ZkConnector m_zkConnector;

	private final String m_groupId;
	private final String m_topic;
	private final int m_partition;
	private final String m_clientId;
	private final KafkaConsumerConfig m_config;
	private final KafkaMessageSerializer m_serializer;
	private final int m_retries = 3;

	private final String m_pstatePath;
	private final String m_offsetPath;

	private Broker m_leader;
	private SimpleConsumer m_consumer;
	private long m_readOffset = -1;

	private int m_nextBatchSizeBytes = -1;

	/**
	 * whether the partition is taken or to be released
	 */
	private final AtomicBoolean m_taken = new AtomicBoolean(true);

	/**
	 * is lost in zookeeper, determines onStreamTermination/onException to be
	 * called
	 */
	private final AtomicBoolean m_lost = new AtomicBoolean(false);

	/**
	 * to control the wait time when nothing fetched
	 */
	private long m_nextFetchInMs = 0;

	/**
	 * onIdle control
	 */
	private long m_idleTimestamp = 0;
	private boolean m_onIdled = false;

	public PartitionReader(String topic, int partition,
			KafkaConsumerConfig config,
			KafkaController.ZkConnector zkConnector,
			KafkaMessageSerializer serializer) {
		this.m_groupId = config.getGroupId();
		this.m_topic = topic;
		this.m_partition = partition;
		this.m_pstatePath = partitionStatePath(topic, partition);
		this.m_offsetPath = offsetPath(m_groupId, topic, partition);
		this.m_clientId = this.m_topic + "_" + this.m_partition;
		this.m_config = config;

		this.m_serializer = serializer;
		this.m_zkConnector = zkConnector;

		init();
	}

	public void initOffset(boolean isNew) {
		if (LOGGER.isInfoEnabled())
			LOGGER.info( "Init offset for " + m_clientId);

		if (isNew) {
			m_readOffset = fetchResetOffset(SmallestTimeString());
			// commit the init offset first, useful to revert
			commitOffset();
		} else {
			if (offsetExists())
				m_readOffset = readZkOffset();
			if (m_readOffset < 0) {
				m_readOffset = fetchResetOffset(m_config.getAutoOffsetReset());
				// commit the init offset first, useful to revert
				commitOffset();
			}
		}
		if (m_readOffset < 0)
			throw new RuntimeException(
					"Fatal errors in getting init offset for " + m_clientId);

		if (LOGGER.isInfoEnabled())
			LOGGER.info( m_clientId + ": init offset is set to "
					+ m_readOffset);
	}

	public void resetOffset() {
		if (LOGGER.isInfoEnabled())
			LOGGER.info( "Reset offset for " + m_clientId);

		m_readOffset = fetchResetOffset(m_config.getAutoOffsetReset());

		if (m_readOffset < 0)
			throw new RuntimeException("Fatal errors in resetting offset for "
					+ m_clientId);
		commitOffset();

		if (LOGGER.isInfoEnabled())
			LOGGER.info( m_clientId + ": offset is reset to "
					+ m_readOffset);
	}

	public void revertOffset() {
		long oldOffset = m_readOffset;
		m_readOffset = readZkOffset();
		if (m_readOffset < 0) {
			LOGGER.warn( m_clientId
					+ "No offset node in zookeeper.");
			resetOffset();
		}
		if (LOGGER.isInfoEnabled())
			LOGGER.info( m_clientId + ": offset is reverted from "
					+ oldOffset + " to " + m_readOffset);
	}

	public long getReadOffset() {
		return m_readOffset;
	}

	public void setReadOffset(long offset) {
		if (LOGGER.isInfoEnabled())
			LOGGER.info( m_clientId + "'s readOffset is set to "
					+ offset + " from " + m_readOffset);
		m_readOffset = offset;
	}

	public void setNextBatchSizeBytes(int nextBatchSizeBytes) {
		if (nextBatchSizeBytes < 0)
			return;
		this.m_nextBatchSizeBytes = nextBatchSizeBytes;
	}

	public boolean isTaken() {
		return m_taken.get();
	}

	/**
	 * set to release and ConsumeTask will do it in next round
	 */
	public void setToRelease() {
		m_taken.set(false);
	}

	public void setLost() {
		m_lost.set(true);
	}

	public boolean isLost() {
		return m_lost.get();
	}

	public String getTopic() {
		return m_topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public String getId() {
		return m_clientId;
	}

	public long calcWaitTime() {
		return m_nextFetchInMs - System.currentTimeMillis();
	}

	public long getNextFetchInMs() {
		return m_nextFetchInMs;
	}

	public void setNextFetchInMs(long nextFetchInMs) {
		this.m_nextFetchInMs = nextFetchInMs;
	}

	public void fistIdle() {
		this.m_idleTimestamp = System.currentTimeMillis();
	}

	public long getIdleTime() {
		return System.currentTimeMillis() - m_idleTimestamp;
	}

	public boolean isIdled() {
		return this.m_idleTimestamp > 0;
	}

	public void setOnIdled() {
		this.m_idleTimestamp = 0;
		this.m_onIdled = true;
	}

	public boolean isOnIdled() {
		return this.m_onIdled;
	}

	public void clearIdleStats() {
		this.m_idleTimestamp = 0;
		this.m_onIdled = false;
	}

	/**
	 * consumer events from kafka by one FetchRequest
	 */
	public List<JetstreamEvent> readEvents() throws OffsetOutOfRangeException {
		List<JetstreamEvent> events = new ArrayList<JetstreamEvent>();

		if (m_nextBatchSizeBytes < 0)
			m_nextBatchSizeBytes = m_config.getBatchSizeBytes();

		if (m_nextBatchSizeBytes == 0) {
			// nextBatchSize only affects one fetch
			m_nextBatchSizeBytes = m_config.getBatchSizeBytes();
			return events;
		}

		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < m_retries; j++) {
				FetchRequest req = new FetchRequestBuilder()
						.clientId(m_clientId)
						.addFetch(m_topic, m_partition, m_readOffset,
								m_nextBatchSizeBytes).build();

				FetchResponse fetchResponse = null;
				try {
					fetchResponse = m_consumer.fetch(req);
				} catch (Exception e) {
					LOGGER.error( "Error occurs when fetching.", e);
					continue;
				}
				if (fetchResponse == null)
					continue;
				if (fetchResponse.hasError()) {
					short code = fetchResponse.errorCode(m_topic, m_partition);
					if (code == ErrorMapping.OffsetOutOfRangeCode()) {
						long smallest = -1L, largest = -1L;
						try {
							smallest = fetchResetOffset(SmallestTimeString());
							largest = fetchResetOffset(LargestTimeString());
						} catch (Exception e) {
						}
						throw new OffsetOutOfRangeException(m_clientId
								+ ": readOffset=" + m_readOffset + " smallest="
								+ smallest + " largest=" + largest);
					} else
						continue;
				} else {
					ByteBufferMessageSet messageSet = fetchResponse.messageSet(
							m_topic, m_partition);

					boolean hasMessage = messageSet.iterator().hasNext();
					if (!hasMessage && !readToTheEnd()) {
						m_nextBatchSizeBytes = Math.min(
								m_nextBatchSizeBytes * 2,
								m_config.getMaxBatchSizeBytes());
						continue;
					}

					for (MessageAndOffset messageAndOffset : messageSet) {
						long currentOffset = messageAndOffset.offset();
						if (currentOffset < m_readOffset) {
							continue;
						}
						m_readOffset = messageAndOffset.nextOffset();
						Message message = messageAndOffset.message();
						ByteBuffer k = message.key();
						ByteBuffer p = message.payload();
						byte[] key = null;
						if (k != null) {
							key = new byte[k.limit()];
							k.get(key);
						}
						byte[] payload = new byte[p.limit()];
						p.get(payload);
						JetstreamEvent event = m_serializer
								.decode(key, payload);
						EventKafkaMetadata meta = new EventKafkaMetadata(
								m_topic, m_partition, currentOffset);
						event.put(kmKey, meta.encode());
						events.add(event);
					}

					// nextBatchSize only affects one fetch
					m_nextBatchSizeBytes = m_config.getBatchSizeBytes();
					return events;
				}
			}
			// cannot get events after retries, reinit and try again
			reinit();
		}
		throw new RuntimeException("Fail to read events from " + m_clientId
				+ " with offset " + m_readOffset);
	}

	public void commitOffset() {
		if (!m_zkConnector.exists(m_offsetPath))
			m_zkConnector.create(m_offsetPath, false);
		m_zkConnector.writeString(m_offsetPath, String.valueOf(m_readOffset));

		if (LOGGER.isInfoEnabled())
			LOGGER.info( m_clientId + ": offset is committed with "
					+ m_readOffset);
	}

	public void close() {
		m_consumer.close();
	}

	private boolean offsetExists() {
		return m_zkConnector.exists(m_offsetPath);
	}

	public long readZkOffset() {
		try {
			if (!offsetExists())
				return -1;
			String offsetStr = m_zkConnector.readString(m_offsetPath);
			if (offsetStr == null)
				return -1;
			return Long.parseLong(offsetStr);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private boolean readToTheEnd() {
		return m_readOffset >= getLargestOffset();
	}

	private long getLargestOffset() {
		return fetchResetOffset(LargestTimeString());
	}

	private long fetchResetOffset(String reset) {

		long time = LatestTime();
		if (reset != null && reset.equals(SmallestTimeString()))
			time = EarliestTime();

		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		TopicAndPartition tp = new TopicAndPartition(m_topic, m_partition);
		PartitionOffsetRequestInfo info = new PartitionOffsetRequestInfo(time,
				1);
		requestInfo.put(tp, info);
		OffsetRequest request = new OffsetRequest(requestInfo,
				CurrentVersion(), m_clientId);

		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < m_retries; j++) {
				OffsetResponse response = m_consumer.getOffsetsBefore(request);
				if (response.hasError()) {
					short errorCode = response.errorCode(m_topic, m_partition);
					LOGGER.warn(
							"Error when fetch offset from kafka, errorCode="
									+ errorCode);
					continue;
				}
				long[] offsets = response.offsets(m_topic, m_partition);
				if (offsets.length <= 0)
					continue;
				return offsets[0];
			}
			// cannot get offset after retries, reinit and try again
			reinit();
		}
		throw new RuntimeException("Fail to get resetOffset " + reset
				+ " after retries for " + m_clientId);
	}

	private void reinit() {
		if (m_consumer != null)
			m_consumer.close();
		init();
	}

	private void init() {
		m_taken.set(true);
		this.m_leader = getLeader();
		this.m_consumer = new SimpleConsumer(m_leader.host(), m_leader.port(),
				m_config.getSocketTimeoutMs(),
				m_config.getSocketReceiveBufferBytes(), m_clientId);
		this.m_nextBatchSizeBytes = m_config.getBatchSizeBytes();
	}

	private Broker getLeader() {
		try {
			Map<String, Object> state = m_zkConnector.readJSON(m_pstatePath);
			Integer brokerId = ((Number) state.get("leader")).intValue();
			Map<String, Object> brokerData = m_zkConnector
					.readJSON(brokerPath(brokerId));
			if (brokerData == null)
				throw new RuntimeException("Broker info not found for "
						+ m_clientId);
			String host = (String) brokerData.get("host");
			Integer port = ((Number) brokerData.get("port")).intValue();
			return new Broker(brokerId, host, port);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private String partitionStatePath(String topic, int partition) {
		StringBuffer sb = new StringBuffer("/brokers/topics/");
		sb.append(topic).append("/partitions/");
		sb.append(partition).append("/state");
		return sb.toString();
	}

	private String brokerPath(int brokerId) {
		StringBuffer sb = new StringBuffer("/brokers/ids/");
		sb.append(brokerId);
		return sb.toString();
	}

	private String offsetPath(String groupId, String topic, int partition) {
		StringBuffer sb = new StringBuffer("/consumers/");
		sb.append(groupId).append("/offsets/");
		sb.append(topic).append("/").append(partition);
		return sb.toString();
	}

	@Override
	public int compareTo(PartitionReader p) {
		if (m_nextFetchInMs < p.getNextFetchInMs())
			return -1;
		else if (m_nextFetchInMs > p.getNextFetchInMs())
			return 1;
		else
			return 0;
	}

}
