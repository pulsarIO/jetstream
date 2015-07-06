/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.stats;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.PartitionKey;

/**
 * @author weifang
 * 
 */
public abstract class EventTsBasedStatsRecorder extends BaseStatsRecorder {

	@Override
	public synchronized void onFilesCreated(PartitionKey key, long startOffset,
			String resolvedFolder, Collection<String> eventTypes,
			String tmpFileName) {
		super.onFilesCreated(key, startOffset, resolvedFolder, eventTypes,
				tmpFileName);
		EventTsBasedStats stats = (EventTsBasedStats) getStats(key);
		for (String eventType : eventTypes) {
			stats.getMinTimestamps().put(eventType, Long.MAX_VALUE);
			stats.getMaxTimestamps().put(eventType, 0L);
			stats.getTotalLatencies().put(eventType, 0L);
		}
	}

	@Override
	public void onEventWritten(PartitionKey key, String eventTypeCategory,
			JetstreamEvent event) {
		super.onEventWritten(key, eventTypeCategory, event);
		Long ts = getTimestamp(event);
		if (ts != null) {
			EventTsBasedStats stats = (EventTsBasedStats) getStats(key);
			Long v = null;
			v = stats.getMinTimestamps().get(eventTypeCategory);
			if (v != null && v > ts)
				stats.getMinTimestamps().put(eventTypeCategory, ts);
			v = stats.getMaxTimestamps().get(eventTypeCategory);
			if (v != null && v < ts)
				stats.getMaxTimestamps().put(eventTypeCategory, ts);
			long latency = System.currentTimeMillis() - ts;
			stats.incLatency(eventTypeCategory, latency);
		}
	}

	protected abstract Long getTimestamp(JetstreamEvent event);

	@Override
	protected BaseStats createStast() {
		return new EventTsBasedStats();
	}

	public static class EventTsBasedStats extends BaseStats {
		private final Map<String, Long> minTimestamps = new LinkedHashMap<String, Long>();
		private final Map<String, Long> maxTimestamps = new LinkedHashMap<String, Long>();
		private final Map<String, Long> totalLatencies = new LinkedHashMap<String, Long>();

		@Override
		public void cleanup() {
			super.cleanup();
			minTimestamps.clear();
			maxTimestamps.clear();
			totalLatencies.clear();
		}

		public Map<String, Long> getMinTimestamps() {
			return minTimestamps;
		}

		public Map<String, Long> getMaxTimestamps() {
			return maxTimestamps;
		}

		public Map<String, Long> getTotalLatencies() {
			return totalLatencies;
		}

		public void incLatency(String eventType, long latency) {
			Long v = totalLatencies.get(eventType);
			if (v != null) {
				v += latency;
				totalLatencies.put(eventType, v);
			}
		}

	}

}
