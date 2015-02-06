/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.ApplicationEvent;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.BatchSource;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.EventMetaInfo;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractBatchEventProcessor;

/**
 * a simple implementation of auto-commit mode kafka consumer should be linked
 * to InboundKafkaChannel
 * 
 * @author xiaojuwu1
 */
public class SimpleKafkaProcessor extends AbstractBatchEventProcessor {

	private SimpleKafkaProcessorConfig m_config;

	private Map<String, Long> m_lastAdvanceTimes = new ConcurrentHashMap<String, Long>();

	private Map<String, List<JetstreamEvent>> cachedBatches = new ConcurrentHashMap<String, List<JetstreamEvent>>();

	private Map<String, Long> waitForLastBatch = new ConcurrentHashMap<String, Long>();

	@Override
	public BatchResponse onNextBatch(BatchSource source,
			Collection<JetstreamEvent> events) throws EventException {
		initAutoAdvanceTime(source);

		incrementEventRecievedCounter(events.size());

		if (m_config.isNoDuplication()) {
			cacheBatch(source, events);
		} else {
			BatchResponse ret = sendBatch(source, events);
			if (ret != null)
				return ret;
		}

		BatchResponse ret = autoAdvanceResponse(source);

		// check if the read rate exceeds the max, if exceeds, make the next
		// batch wait for a while
		String key = getKey(source);
		Long waitMs = waitForLastBatch.get(key);
		if (waitMs != null && waitMs > 0) {
			ret.setWaitTimeInMs(waitMs);
		}

		return ret;
	}

	protected void cacheBatch(BatchSource source,
			Collection<JetstreamEvent> events) {
		String key = getKey(source);
		List<JetstreamEvent> cache = cachedBatches.get(key);
		if (cache == null) {
			cache = new ArrayList<JetstreamEvent>();
			cachedBatches.put(key, cache);
		}
		cache.addAll(events);
	}

	protected BatchResponse sendBatch(BatchSource source,
			Collection<JetstreamEvent> events) {
		long start = System.currentTimeMillis();

		// send to batch event sinks
		if (!getBatchEventSinks().isEmpty()) {
			try {
				simplySendEvents(events);
			} catch (Throwable e) {
				incrementEventDroppedCounter(events.size());
				return BatchResponse.getNextBatch().setOffset(
						source.getHeadOffset());
			}
		}

		// send to non-batch event sinks
		if (!getEventSinks().isEmpty()) {
			long startOffset = source.getHeadOffset();
			int index = 0;
			try {
				Iterator<JetstreamEvent> it = events.iterator();

				while (it.hasNext()) {
					JetstreamEvent event = it.next();
					simplySendEvent(event);
					index++;
				}
			} catch (Throwable e) {
				super.incrementEventDroppedCounter(events.size() - index);
				return BatchResponse.getNextBatch().setOffset(
						startOffset + index);
			}
		}

		handleReadRate(start, source, events.size());

		return null;
	}

	private void handleReadRate(long start, BatchSource source, int eventCount) {
		if (m_config.getMaxReadRate() > 0) {
			long end = System.currentTimeMillis();
			int tasks;
			String key = getKey(source);
			if (waitForLastBatch.containsKey(key)) {
				tasks = waitForLastBatch.size();
			} else {
				tasks = waitForLastBatch.size() + 1;
			}
			long durInRate = (1000 * eventCount * tasks) / m_config.getMaxReadRate();
			long waitMs = durInRate - (end - start);
			waitForLastBatch.put(key, waitMs);
		}
	}

	protected void simplySendBatch(BatchSource source,
			Collection<JetstreamEvent> events) {
		long start = System.currentTimeMillis();

		// send to batch event sinks
		if (!getBatchEventSinks().isEmpty()) {
			try {
				simplySendEvents(events);
			} catch (Throwable e) {
				incrementEventDroppedCounter(events.size());
			}
		}

		// send to non-batch event sinks
		if (!getEventSinks().isEmpty()) {
			Iterator<JetstreamEvent> it = events.iterator();

			while (it.hasNext()) {
				try {
					JetstreamEvent event = it.next();
					simplySendEvent(event);
				} catch (Throwable e) {
					super.incrementEventDroppedCounter();
				}
			}
		}

		handleReadRate(start, source, events.size());

	}

	protected void simplySendEvents(Collection<JetstreamEvent> events) {
		EventMetaInfo meta = new EventMetaInfo();
		super.fireSendEvents(events, meta);
		incrementEventSentCounter(events.size());
	}

	protected void simplySendEvent(JetstreamEvent event) {
		super.fireSendEvent(event);
		incrementEventSentCounter();
	}

	@Override
	public void onBatchProcessed(BatchSource source) {
		if (m_config.isNoDuplication()) {
			String key = getKey(source);
			List<JetstreamEvent> cached = cachedBatches.get(key);
			if (cached != null) {
				simplySendBatch(source, cached);
				cached.clear();
			}
		}
	}

	@Override
	public BatchResponse onStreamTermination(BatchSource source)
			throws EventException {
		return BatchResponse.advanceAndGetNextBatch();
	}

	@Override
	public BatchResponse onException(BatchSource source, Exception ex) {
		return BatchResponse.getNextBatch().setOffset(source.getHeadOffset());
	}

	@Override
	public BatchResponse onIdle(BatchSource source) {
		return autoAdvanceResponse(source);
	}

	protected void initAutoAdvanceTime(BatchSource source) {
		String key = getKey(source);
		if (!m_lastAdvanceTimes.containsKey(key))
			m_lastAdvanceTimes.put(key, System.currentTimeMillis());
	}

	protected BatchResponse autoAdvanceResponse(BatchSource source) {
		if (isAutoAdvanceEveryBatch()) {
			return BatchResponse.advanceAndGetNextBatch();
		} else {
			String key = new StringBuilder().append(source.getTopic())
					.append("-").append(source.getPartition()).toString();
			Long lastTs = m_lastAdvanceTimes.get(key);
			long curTime = System.currentTimeMillis();
			boolean advance = false;
			if (lastTs != null
					&& (curTime - lastTs.longValue()) >= m_config
							.getAutoAdvanceInterval()) {
				advance = true;
				m_lastAdvanceTimes.put(key, curTime);
			}

			if (advance) {
				return BatchResponse.advanceAndGetNextBatch();
			} else {
				return BatchResponse.getNextBatch();
			}
		}
	}

	@Override
	public void init() throws Exception {
		if (m_config.isNoDuplication() && !isAutoAdvanceEveryBatch())
			throw new RuntimeException(
					"NoDuplication only applies to AUTO_ADVANCE_EVERYBATCH mode.");
	}

	@Override
	public void pause() {
	}

	@Override
	public void resume() {
	}

	@Override
	public void shutDown() {
	}

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {

			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

			if (bcInfo.isChangedBean(m_config)) {
				setConfig((SimpleKafkaProcessorConfig) bcInfo.getChangedBean());
			}
		}
	}

	public SimpleKafkaProcessorConfig getConfig() {
		return m_config;
	}

	public void setConfig(SimpleKafkaProcessorConfig config) {
		this.m_config = config;
	}

	private boolean isAutoAdvanceEveryBatch() {
		return SimpleKafkaProcessorConfig.AUTO_ADVANCE_EVERYBATCH
				.equals(m_config.getAutoAdvanceMode());
	}

	private String getKey(BatchSource source) {
		return new StringBuilder().append(source.getTopic()).append("-")
				.append(source.getPartition()).toString();
	}

}
