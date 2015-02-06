/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static com.ebay.jetstream.event.BatchSourceCommand.OnBatchProcessed;
import static com.ebay.jetstream.event.BatchSourceCommand.OnException;
import static com.ebay.jetstream.event.BatchSourceCommand.OnIdle;
import static com.ebay.jetstream.event.BatchSourceCommand.OnNextBatch;
import static com.ebay.jetstream.event.BatchSourceCommand.OnStreamTermination;
import static com.ebay.jetstream.event.channel.kafka.KafkaConstants.COMMON_WAIT_INTERVAL;
import static com.ebay.jetstream.event.channel.kafka.KafkaConstants.COORDINATION_RETRY_INTERVAL;

import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;

import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.common.NameableThreadFactory;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.event.BatchEventSink;
import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.BatchSource;
import com.ebay.jetstream.event.BatchSourceCommand;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.EventMetaInfo;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.AbstractBatchInboundChannel;
import com.ebay.jetstream.event.support.AbstractBatchEventProcessor;
import com.ebay.jetstream.management.Management;

/**
 * 
 * @author xiaojuwu1, weifang
 * 
 */
@ManagedResource(objectName = "Event/Channel", description = "Kafka Batch Inbound Channel")
public class InboundKafkaChannel extends AbstractBatchInboundChannel implements
		KafkaConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(InboundKafkaChannel.class.getName());

	private final BlockingQueue<PartitionReader> m_queue = new LinkedBlockingQueue<PartitionReader>();

	public class ConsumerTask implements Runnable {

		private String taskId;

		/**
		 * partitions to hold and read
		 */
		private LinkedList<PartitionReader> hold = new LinkedList<PartitionReader>();

		private List<JetstreamEvent> cache = new ArrayList<JetstreamEvent>();

		@Override
		public void run() {
			taskId = Thread.currentThread().getName();
			boolean zkDowned = false;
			while (runningFlag.get()) {
				PartitionReader preader = null;
				try {
					if (isPaused()) {
						Thread.sleep(COMMON_WAIT_INTERVAL);
						continue;
					}

					cache.clear();

					// check whether to release hold partitions
					checkRelease();

					preader = m_queue.poll();
					if (preader == null) {

						if (hold.isEmpty()) {
							preader = m_queue.take();

						} else {
							PartitionReader earliest = hold.getFirst();
							long wait = earliest.calcWaitTime();
							if (wait > 0) {
								preader = m_queue.poll(wait,
										TimeUnit.MILLISECONDS);
							}
							if (preader == null)
								preader = hold.poll();
						}
					}

					// check zk down.
					if (!m_zkConnector.isZkConnected()) {
						try {
							zkDowned = true;
							LOGGER.error(
									"Detected ZK down. Task not working: "
											+ taskId);
							Thread.sleep(COMMON_WAIT_INTERVAL);
						} catch (InterruptedException e) {
						}
						continue;
					} else {
						if (zkDowned) {
							LOGGER.info(
									taskId
											+ " : Zk is back and continue consumer task.");
						}
						zkDowned = false;
					}

					// if not exceed timeout, continue to hold
					if (preader.calcWaitTime() > 0) {
						hold(preader);
						preader = null;
						continue;
					}

					cache = preader.readEvents();

					if (cache.size() == 0) {
						if (LOGGER.isDebugEnabled())
							LOGGER.debug( "No events read from "
									+ preader.getId() + " with offset "
									+ preader.getReadOffset());

						checkIdle(preader);

						// hold no-events partitions
						hold(preader);
						preader.setNextFetchInMs(System.currentTimeMillis()
								+ m_config.getFetchWaitMaxMs());
						preader = null;

					} else {
						incrementEventRecievedCounter(cache.size());
						preader.clearIdleStats();

						// release all the hold partitions
						while (!hold.isEmpty()) {
							PartitionReader p = hold.poll();
							putToQueue(p);
						}

						// flush events stream
						onNextBatch(preader);
					}

				} catch (OffsetOutOfRangeException e) {
					LOGGER.error( "OffsetOutOfRange when fetching "
							+ preader.getId(), e);
					this.handleOffsetOutOfRange(preader, e);

				} catch (Throwable ex) {
					if (ex.getCause() instanceof ConnectionLossException) {
						LOGGER.error(
								"Zookeeper connection is lost.", ex);
						if (preader != null) {
							this.handleZkConnectionLoss(preader,
									(Exception) ex.getCause());
						}

					} else if (ex instanceof ClosedByInterruptException
							|| ex instanceof InterruptedException
							|| ex.getCause() instanceof InterruptedException) {

						if (LOGGER.isInfoEnabled())
							LOGGER.info( taskId + " is interrupted.",
									ex);

					} else {
						LOGGER.error( ex.getMessage(), ex);
						registerError(ex);
					}

				} finally {
					if (preader != null) {
						if (preader.isTaken()) {
							// put back partition having events
							putToQueue(preader);
						} else {
							finish(preader);
						}
						preader = null;
					}
				}

			}

			// gracefully shutdown, commit and release all
			this.shutdown();
		}

		private void checkIdle(PartitionReader p) {
			if (p.isOnIdled())
				return;
			if (p.isIdled()) {
				if (p.getIdleTime() > m_config.getIdleTimeInMs()) {
					onIdle(p);
					p.setOnIdled();
				}
			} else {
				p.fistIdle();
			}
		}

		// hold partition by nextFetchInMs order
		private void hold(PartitionReader p) {
			int index = 0;
			for (index = 0; index < hold.size(); index++) {
				if (p.compareTo(hold.get(index)) < 0) {
					hold.add(index, p);
					break;
				}
			}
			if (index == hold.size())
				hold.addLast(p);
		}

		private void handleOffsetOutOfRange(PartitionReader p, Exception e) {
			try {
				BatchResponse res = onException(p, e);
				// won't handle res offset, always reset offset
				if (res != null)
					res.setRequest(null);
				handleResponse(p, 0, res);
				p.resetOffset();

			} catch (Throwable ex) {
				LOGGER.error(
						"Error occurs when handle OffsetOutOfRangeException for "
								+ p.getId(), ex);
			}
		}

		private void handleZkConnectionLoss(PartitionReader p, Exception e) {
			try {
				BatchResponse res = onException(p, e);
				handleResponse(p, 0, res);

			} catch (Throwable ex) {
				LOGGER.error(
						"Error occurs when handle Zk Connection Loss for "
								+ p.getId(), ex);
			}
		}

		/**
		 * check whether to release the hold partitions
		 */
		private void checkRelease() {
			List<PartitionReader> toRelease = new ArrayList<PartitionReader>();
			for (PartitionReader p : hold) {
				if (!p.isTaken()) {
					toRelease.add(p);
					finish(p);
				}
			}
			hold.removeAll(toRelease);
		}

		private void putToQueue(PartitionReader p) {
			Exception ex = null;
			for (int i = 0; i < 2; i++) {
				try {
					m_queue.put(p);
					ex = null;
					break;
				} catch (InterruptedException e) {
					LOGGER.warn(
							"Interrupted when put " + p.getId()
									+ " back to queue, try again.", e);
					ex = e;
					continue;
				}
			}
			if (ex != null) {
				LOGGER.error( "Fail to put " + p.getId()
						+ " back to queue.", ex);
				registerError(ex);
			}
		}

		private void onNextBatch(PartitionReader p) {
			int batchSize = cache.size();

			// send events
			EventMetaInfo meta = genEventMeta(OnNextBatch, p, batchSize);
			try {
				fireSendEvents(cache, meta);
				incrementEventSentCounter(batchSize);
				long start = p.getReadOffset() - batchSize;
				long end = p.getReadOffset() - 1;

				if (LOGGER.isDebugEnabled())
					LOGGER.debug( "Send events of " + p.getId()
							+ " from " + start + " to " + end);

			} catch (Exception e) {
				LOGGER.error(
						"Excpetion in EventSink during flush.", e);
				registerError(e);
				incrementEventDroppedCounter(batchSize);
				BatchResponse res = onException(p, e);
				handleResponse(p, 0, res);
				return;
			}

			this.handleResponse(p, batchSize, meta.getBatchResponse());
		}

		private void onBatchProcessed(PartitionReader p) {
			EventMetaInfo meta = genEventMeta(OnBatchProcessed, p);
			try {
				fireSendEvents(null, meta);
			} catch (Exception e) {
				LOGGER.error(
						"Error occurs during OnBatchProcessed.", e);
				registerError(e);
			}
		}

		private BatchResponse onStreamTermination(PartitionReader p) {
			EventMetaInfo meta = genEventMeta(OnStreamTermination, p);
			try {
				fireSendEvents(null, meta);
				if (LOGGER.isDebugEnabled())
					LOGGER.debug( " Terminate stream of " + p.getId());

			} catch (Exception e) {
				LOGGER.error(
						"Excpetion occurs during streamTermination.", e);
				registerError(e);
				onException(p, e);
				return null;
			}
			return meta.getBatchResponse();
		}

		/**
		 * send drop stream signal
		 */
		private BatchResponse onException(PartitionReader p, Exception ex) {
			EventMetaInfo meta = genEventMeta(OnException, p, ex);
			try {
				fireSendEvents(null, meta);
				if (LOGGER.isInfoEnabled())
					LOGGER.info(
							taskId + " on exception for " + p.getId(), ex);
			} catch (Throwable e) {
				LOGGER.error( taskId + " fail to on exception for "
						+ p.getId(), e);
				registerError(e);
			}

			return meta.getBatchResponse();
		}

		private void onIdle(PartitionReader p) {
			EventMetaInfo meta = genEventMeta(OnIdle, p);
			try {
				fireSendEvents(null, meta);
			} catch (Throwable e) {
				LOGGER.error(
						"Error occurs when invoke onIdle() for " + p.getId(), e);
				registerError(e);
			}

			this.handleResponse(p, 0, meta.getBatchResponse());
		}

		private void handleResponse(PartitionReader p, int batchSize,
				BatchResponse res) {
			// handle response
			if (res != null) {
				if (res.getWaitTimeInMs() > 0)
					p.setNextFetchInMs(System.currentTimeMillis()
							+ res.getWaitTimeInMs());

				int resBatchSize = res.getBatchSizeBytes();
				if (resBatchSize >= 0
						&& resBatchSize <= m_config.getMaxBatchSizeBytes())
					p.setNextBatchSizeBytes(res.getBatchSizeBytes());

				long offset = res.getOffset();

				if (res.getRequest() == null)
					return;

				switch (res.getRequest()) {
				case GetNextBatch:
					if (offset >= 0) {
						p.setReadOffset(offset);
					}
					break;
				case AdvanceAndGetNextBatch:
					if (offset >= 0) {
						p.setReadOffset(offset);
					}

					if (retryCommitOrRevert(true, p)) {
						onBatchProcessed(p);
					}

					break;
				case RevertAndGetNextBatch:
					if (offset >= 0) {
						p.setReadOffset(offset);
					} else {
						retryCommitOrRevert(false, p);
					}
					break;
				}
			} else {
				LOGGER.warn( "No BatchResponse returned!");
			}
		}

		private boolean retryCommitOrRevert(boolean isCommit, PartitionReader p) {
			boolean downed = false;
			boolean ret = false;
			while (true) {
				boolean downOnce = false;
				try {
					if (!downed
							|| m_zkCoordinator.isZkOwnerExist(p.getTopic(),
									p.getPartition())) {
						if (m_zkConnector.isZkConnected()) {
							if (isCommit) {
								p.commitOffset();
							} else {
								p.revertOffset();
							}
							ret = true;
						} else {
							downOnce = true;
						}
					}

					break;
				} catch (Throwable th) {
					downOnce = true;
				}

				if (downOnce) {
					downed = true;
					try {
						Thread.sleep(COMMON_WAIT_INTERVAL);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
					if (!runningFlag.get()) {
						break;
					}
				}
			}

			if (downed) {
				LOGGER.info( taskId
						+ " : Zk is back and continue the change to offset.");
			}

			return ret;
		}

		/**
		 * release partition before finishing task
		 */
		private void finish(PartitionReader p) {
			try {
				if (p.isLost() || !m_zkConnector.isZkConnected()) {
					Exception ex = new PartitionLostException(
							m_consumerId.toString(), p.getTopic(),
							p.getPartition());
					LOGGER.error(
							"Error occurs when finish partition", ex);
					onException(p, ex);
				} else {
					BatchResponse res = onStreamTermination(p);
					if (res != null)
						handleResponse(p, 0, res);
				}

			} catch (Throwable e) {
				LOGGER.error( "Error occurs when finish partition "
						+ p.getId(), e);
				registerError(e);
			} finally {
				unsubscribePartition(p);
			}
		}

		/**
		 * gracefully shutdown when kafka channel is unsubscribed commit and
		 * release all partitions
		 */
		private void shutdown() {
			try {
				if (!runningFlag.get()) {
					// finish all hold partitions
					while (!hold.isEmpty()) {
						PartitionReader p = hold.pollFirst();
						if (p == null) {
							break;
						} else {
							finish(p);
						}
					}
					// finish partitions in the queue
					while (!m_queue.isEmpty()) {
						PartitionReader p = m_queue.poll();
						if (p == null) {
							break;
						} else {
							finish(p);
						}
					}

					if (LOGGER.isInfoEnabled())
						LOGGER.info( taskId
								+ " is gracefully shutdown!");
				} else {
					LOGGER.error(
							taskId
									+ " try to gracefully shutdown but runningFlag is true.");
				}
			} catch (Throwable e) {
				LOGGER.error(
						"Error occurs when gracefully shutdown kafkaConsumer thread "
								+ taskId, e);
			}

		}

		public LinkedList<PartitionReader> getHold() {
			return hold;
		}

		public String getId() {
			return taskId;
		}

	}

	@Override
	public void zkReconnected() {
		try {
			m_zkCoordinator.reRegisterConsumer(true);
			List<String> topics = m_channelAddress.getChannelTopics();
			for (String topic : topics) {
				Set<Integer> myPartitions = m_zkCoordinator
						.getMyPartitions(topic);

				if (LOGGER.isInfoEnabled())
					LOGGER.info( "Partitions in zk for " + topic
							+ ": " + myPartitions);

				for (int partition : myPartitions) {
					TopicAndPartition key = new TopicAndPartition(topic,
							partition);
					PartitionReader p = m_partitionMap.get(key);
					boolean reset = m_zkCoordinator
							.resetOwner(topic, partition);
					if (!reset) {
						p.setLost();
						p.setToRelease();
					}
				}
			}
		} catch (Throwable th) {
			LOGGER.error(
					"Error occurs when rewriting ephemeral nodes after zk down");
		}
	}

	@Override
	public void coordinate() {
		synchronized (lock) {
			if (!runningFlag.get()) {
				if (LOGGER.isInfoEnabled())
					LOGGER.info( m_config.getGroupId()
							+ " is not runnable, no need to coordinate.");
				return;
			}

			m_zkCoordinator.reRegisterConsumer(false);

			LOGGER.info(
					"Coordinate taken partitions with that in zookeeper for "
							+ m_consumerId + ", partitions in map "
							+ getMapPartitions());

			List<String> topics = m_channelAddress.getChannelTopics();
			for (String topic : topics) {
				Set<Integer> myPartitions = m_zkCoordinator
						.getMyPartitions(topic);

				if (LOGGER.isInfoEnabled())
					LOGGER.info( "Partitions in zk for " + topic
							+ ": " + myPartitions);

				for (int partition : myPartitions) {
					try {
						TopicAndPartition key = new TopicAndPartition(topic,
								partition);
						PartitionReader p = m_partitionMap.get(key);
						if (p == null) {
							takePartition(topic, partition);
						}
					} catch (Throwable ex) {
						String key = new TopicAndPartition(topic, partition)
								.toString();
						LOGGER.error(
								"Error occurs when taking partition " + key, ex);
						registerError(ex);
					}
				}

				for (TopicAndPartition tp : m_partitionMap.keySet()) {
					if (!topic.equals(tp.topic()))
						continue;
					int partition = tp.partition();
					if (!myPartitions.contains(partition)) {
						LOGGER.info( "Try to release " + tp
								+ " in coordinate. ");
						PartitionReader preader = m_partitionMap.get(tp);
						preader.setLost();
						preader.setToRelease();
					}
				}
			}

		}
	}

	@Override
	public Map<String, Integer> calcRebalance() {
		synchronized (lock) {
			if (!runningFlag.get() || isPaused()) {
				if (LOGGER.isInfoEnabled())
					LOGGER.info( m_config.getGroupId()
							+ " is not runnable, no need to rebalance.");
				return null;
			}

			return m_zkCoordinator.getRebalanceCount();
		}
	}

	@Override
	public boolean takePartitions(String topic, int count) {
		synchronized (lock) {
			if (!runningFlag.get() || isPaused()) {
				if (LOGGER.isInfoEnabled())
					LOGGER.info( m_config.getGroupId()
							+ " is not runnable, no need to take partitions.");
				return false;
			}

			if (LOGGER.isInfoEnabled())
				LOGGER.info( m_consumerId + " try to take " + count
						+ " partitions for " + topic + " in this rebalance.");

			int haveTaken = 0;
			int retries = 0;
			int maxRetries = m_config.getRebalanceMaxRetries();
			while (retries < maxRetries && haveTaken < count) {
				Set<Integer> idle = m_zkCoordinator.getToTakePartitions(topic);

				for (int partition : idle) {
					String key = new TopicAndPartition(topic, partition)
							.toString();
					try {
						if (m_zkCoordinator.takePartition(topic, partition)) {
							takePartition(topic, partition);
							haveTaken++;
							if (haveTaken >= count)
								break;
						} else {
							LOGGER.warn( "Fail to take partition "
									+ key);
							break;
						}
					} catch (Throwable ex) {
						LOGGER.error(
								"Error occurs when taking partition " + key, ex);
						registerError(ex);
					}
				}
				retries++;
				try {
					Thread.sleep(COORDINATION_RETRY_INTERVAL);
				} catch (InterruptedException e) {
					if (!runningFlag.get())
						return false;
				}
			}
			if (retries >= m_config.getRebalanceMaxRetries()) {
				LOGGER.warn( count + " should be taken, but only "
						+ haveTaken + " taken in this rebalance.");
				return false;
			}

		}
		return true;
	}

	private void takePartition(String topic, int partition) throws Exception {
		TopicAndPartition key = new TopicAndPartition(topic, partition);
		if (!m_partitionMap.containsKey(key)) {
			PartitionReader preader = createPartitionReader(topic, partition);
			boolean isNew = m_zkCoordinator.isNewPartition(topic, partition);
			preader.initOffset(isNew);
			m_queue.put(preader);
			m_partitionMap.put(key, preader);

			if (LOGGER.isInfoEnabled())
				LOGGER.info( m_consumerId + " takes partition " + key);
		} else {
			LOGGER.error( key + " has been in the map and queue of "
					+ m_consumerId + ", will not put it again.");
		}
	}

	protected PartitionReader createPartitionReader(String topic, int partition) {
		return new PartitionReader(topic, partition, m_config, m_zkConnector,
				m_serializer);
	}

	@Override
	public boolean releasePartitions(String topic, int count) {
		synchronized (lock) {
			if (!runningFlag.get() || isPaused()) {
				if (LOGGER.isInfoEnabled())
					LOGGER.info(
							m_config.getGroupId()
									+ " is not runnable, no need to release partitions.");
				return false;
			}
			Set<Integer> toRelease = m_zkCoordinator.getToReleasePartitions(
					topic, count);

			if (LOGGER.isInfoEnabled())
				LOGGER.info( "Try to release " + count
						+ " partitions for " + topic + " in rebalance.");

			for (int partition : toRelease) {
				TopicAndPartition key = new TopicAndPartition(topic, partition);
				PartitionReader preader = m_partitionMap.get(key);
				preader.setToRelease();
			}
			return true;
		}
	}

	@Override
	public void start() {
		if (LOGGER.isInfoEnabled())
			LOGGER.info( m_config.getGroupId()
					+ " starts after kafkaController start.");
		resubscribe();
	}

	@Override
	public void stop() {
		if (LOGGER.isInfoEnabled())
			LOGGER.info( m_config.getGroupId()
					+ " stops after kafkaController stop.");
		unsubscribe();
	}

	private KafkaChannelAddress m_channelAddress;
	private KafkaMessageSerializer m_serializer;
	private ExecutorService m_executor;
	private KafkaConsumerConfig m_config;
	private Object lock = new Object();
	private AtomicBoolean runningFlag = new AtomicBoolean(false);

	private KafkaController m_kafkaController;
	private KafkaController.ZkConnector m_zkConnector;
	private ConsumerId m_consumerId;
	private ZkCoordinator m_zkCoordinator;
	private final Map<TopicAndPartition, PartitionReader> m_partitionMap = new ConcurrentHashMap<TopicAndPartition, PartitionReader>();
	private List<ConsumerTask> m_consumerTasks;

	@Override
	public void afterPropertiesSet() throws Exception {
		Management.addBean(getBeanName(), this);
		validation();
	}

	private void validation() throws Exception {
		if (!getEventSinks().isEmpty())
			throw new RuntimeException(
					"Only batch event sinks can be linked to InboundKafkaChannel.");

		for (BatchEventSink sink : getBatchEventSinks()) {
			if (!(sink instanceof AbstractBatchEventProcessor)) {
				throw new RuntimeException(
						"Only batch event processors can be linked to InboundKafkaChannel.");
			}
		}

		if (getBatchEventSinks().size() > 1)
			throw new RuntimeException(
					"Only one batch event sink can be linked to InboundKafkaChannel.");

		int poolSize = m_config.getPoolSize();
		if (poolSize <= 0)
			throw new RuntimeException("poolSize is not correctly set.");
	}

	private EventMetaInfo genEventMeta(BatchSourceCommand action,
			PartitionReader p) {
		BatchSource source = new BatchSource(p.getTopic(), p.getPartition());
		return new EventMetaInfo(action, source);
	}

	private EventMetaInfo genEventMeta(BatchSourceCommand action,
			PartitionReader p, Exception ex) {
		BatchSource source = new BatchSource(p.getTopic(), p.getPartition());
		return new EventMetaInfo(action, source, ex);
	}

	private EventMetaInfo genEventMeta(BatchSourceCommand action,
			PartitionReader p, int batchSize) {
		BatchSource source = new BatchSource(p.getTopic(), p.getPartition(),
				p.getReadOffset() - batchSize);
		return new EventMetaInfo(action, source);
	}

	@Override
	public void open() {
		super.open();
		if (m_config.getSubscribeOnInit()) {
			resubscribe();
			m_kafkaController.register(getBeanName(), this);
		} else {
			pause();
		}
	}

	@Override
	public void flush() throws EventException {
	}

	@Override
	@ManagedOperation
	public void pause() {
		if (isPaused())
			return;

		if (LOGGER.isInfoEnabled())
			LOGGER.info( m_config.getGroupId() + " is paused.");

		// m_kafkaController.unregister(getBeanName());
		// unsubscribe();
		changeState(ChannelOperationState.PAUSE);

	}

	@Override
	@ManagedOperation
	public void resume() {
		if (isPaused()) {
			if (LOGGER.isInfoEnabled())
				LOGGER.info( m_config.getGroupId() + " is resumed.");

			// resubscribe();
			// m_kafkaController.register(getBeanName(), this);
			changeState(ChannelOperationState.RESUME);
		}
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	public void shutDown() {
		m_kafkaController.unregister(getBeanName());
		unsubscribe();
		LOGGER.warn( "final events sent = " + getTotalEventsSent()
				+ "final total events dropped =" + getTotalEventsDropped()
				+ "final total events received =" + getTotalEventsReceived());
	}

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {

			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

			// Check procuder config change
			if (bcInfo.isChangedBean(m_config)) {
				setConfig((KafkaConsumerConfig) bcInfo.getChangedBean());
				if (!isPaused() && m_zkConnector.isZkConnected())
					resubscribe();
			} else if (bcInfo.isChangedBean(m_channelAddress)) {
				setAddress((KafkaChannelAddress) bcInfo.getChangedBean());
				if (!isPaused() && m_zkConnector.isZkConnected())
					resubscribe();
			}
		}
	}

	@Override
	public KafkaChannelAddress getAddress() {
		return m_channelAddress;
	}

	@ManagedAttribute
	public KafkaConsumerConfig getConsumerConfig() {
		return m_config;
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}

	public String getQueuedPartitions() {
		StringBuffer str = new StringBuffer();
		Iterator<PartitionReader> it = m_queue.iterator();
		while (it.hasNext()) {
			str.append(it.next().getId()).append("; ");
		}
		return str.toString();
	}

	public String getHoldPartitions() {
		StringBuffer str = new StringBuffer();
		for (ConsumerTask task : m_consumerTasks) {
			LinkedList<PartitionReader> hold = task.getHold();
			if (hold.size() > 0) {
				str.append(task.getId()).append(": ");
				Iterator<PartitionReader> it = hold.iterator();
				while (it.hasNext()) {
					str.append(it.next().getId()).append(",");
				}
				str.append(";  ");
			}
		}
		return str.toString();
	}

	public String getMapPartitions() {
		StringBuffer str = new StringBuffer();
		for (PartitionReader p : m_partitionMap.values()) {
			str.append(p.getId()).append(";");
		}
		return str.toString();
	}

	public void setAddress(KafkaChannelAddress channelAddress) {
		this.m_channelAddress = channelAddress;
	}

	public void setConfig(KafkaConsumerConfig config) {
		this.m_config = config;
	}

	public void setSerializer(KafkaMessageSerializer serializer) {
		this.m_serializer = serializer;
	}

	public void setKafkaController(KafkaController controller) {
		this.m_kafkaController = controller;
		this.m_zkConnector = controller.getZkConnector();
	}

	private void resubscribe() {
		while (true) {
			try {
				unsubscribe();
				synchronized (lock) {
					if (m_config.getEnabled()) {

						m_consumerId = new ConsumerId(m_config.getGroupId());

						if (LOGGER.isInfoEnabled())
							LOGGER.info( "Start to subscribe for "
									+ m_consumerId);

						this.prepareZk();

						runningFlag.set(true);

						NameableThreadFactory factory = new NameableThreadFactory(
								m_config.getGroupId());
						m_executor = Executors.newFixedThreadPool(
								m_config.getPoolSize(), factory);

						m_consumerTasks = new CopyOnWriteArrayList<ConsumerTask>();

						for (int i = 0; i < m_config.getPoolSize(); i++) {
							ConsumerTask task = new ConsumerTask();
							m_executor.submit(task);
							m_consumerTasks.add(task);
						}

					}

					return;
				}

			} catch (Throwable ex) {
				LOGGER.error( ex.getMessage(), ex);
				registerError(ex);
			}

			try {
				Thread.sleep(COMMON_WAIT_INTERVAL);
			} catch (InterruptedException e) {
				LOGGER.error( e.getMessage(), e);
			}
		}
	}

	private void prepareZk() throws Exception {
		List<String> topics = m_channelAddress.getChannelTopics();

		if (m_config.isDynamicAllocatePartition()) {
			m_zkCoordinator = new DynamicCoordinator(topics,
					m_config.getGroupId(), m_consumerId.toString(),
					m_zkConnector);

			if (LOGGER.isInfoEnabled())
				LOGGER.info(
						"Dynamic zk coordinator is prepared for "
								+ m_consumerId);

		} else if (m_config.getFixedPartitionCountPerTopic() > 0) {
			m_zkCoordinator = new FixedCountCoordinator(
					m_config.getFixedPartitionCountPerTopic(), topics,
					m_config.getGroupId(), m_consumerId.toString(),
					m_zkConnector);

			if (LOGGER.isInfoEnabled())
				LOGGER.info(
						"Fixed count zk coordinator is prepared for "
								+ m_consumerId);

		} else {
			String allocated = m_config.getAllocatedPartitions();
			m_zkCoordinator = new StaticCoordinator(allocated, topics,
					m_config.getGroupId(), m_consumerId.toString(),
					m_zkConnector);

			if (LOGGER.isInfoEnabled())
				LOGGER.info( "Static zk coordinator is prepared for "
						+ m_consumerId);
		}

		m_zkCoordinator.registerConsumer();

	}

	private void unsubscribe() {
		synchronized (lock) {

			ExecutorService oldExecutor = m_executor;
			if (runningFlag != null) {
				runningFlag.set(false);
			}

			if (!m_partitionMap.isEmpty()) {
				for (PartitionReader preader : m_partitionMap.values()) {
					preader.setToRelease();
				}
			}

			if (oldExecutor != null) {
				try {
					oldExecutor.shutdownNow();
				} catch (Throwable ex) {
					registerError(ex);
				}

				long start = System.currentTimeMillis();
				try {
					// wait for gracefully shutdown each task
					oldExecutor.awaitTermination(300000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					LOGGER.error( "awaitTermination interrupted in "
							+ getBeanName(), e);
				}

				if (!oldExecutor.isTerminated()) {
					LOGGER.error(
							"Timed out when shutdown kafka inbound thread pool: "
									+ m_consumerId);
				} else {
					if (LOGGER.isInfoEnabled())
						LOGGER.info(
								"It takes "
										+ (System.currentTimeMillis() - start)
										+ " ms to gracefully unsubscribe "
										+ m_consumerId);
				}
			}

			// queue and pmap can only be cleared after partitions in them are
			// committed and finished
			if (!m_queue.isEmpty() || !m_partitionMap.isEmpty()) {
				LOGGER.error(
						"Fail to gracefully finish all partitions before unsubscribe: "
								+ m_consumerId + ", queueSize="
								+ m_queue.size() + ", pmapSize="
								+ m_partitionMap.size());

				for (PartitionReader p : m_queue) {
					this.unsubscribePartition(p);
				}

				for (PartitionReader p : m_partitionMap.values()) {
					this.unsubscribePartition(p);
				}
			}

			m_queue.clear();
			m_partitionMap.clear();

			try {
				if (m_zkCoordinator != null)
					m_zkCoordinator.unregisterConsumer();
			} catch (Throwable th) {
				LOGGER.error( "Error unregistering consumer: "
						+ m_consumerId);
				registerError(th);
			}

			m_zkCoordinator = null;
			m_executor = null;
		}
	}

	private void unsubscribePartition(PartitionReader p) {
		try {
			p.close();
			if (m_zkCoordinator != null) {
				m_zkCoordinator
						.releasePartition(p.getTopic(), p.getPartition());
			}

		} catch (Throwable e) {
			LOGGER.error(
					"Error occurs when unsubscribing partition " + p.getId(), e);
		} finally {
			TopicAndPartition key = new TopicAndPartition(p.getTopic(),
					p.getPartition());
			m_partitionMap.remove(key);

			if (LOGGER.isInfoEnabled())
				LOGGER.info( key + " released.");
		}
	}

}
