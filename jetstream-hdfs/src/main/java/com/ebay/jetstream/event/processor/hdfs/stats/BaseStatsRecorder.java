/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.stats;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.BatchListener;
import com.ebay.jetstream.event.processor.hdfs.PartitionKey;

/**
 * @author weifang
 * 
 */
public abstract class BaseStatsRecorder extends AbstractNamedBean implements
		BatchListener {
	private Map<PartitionKey, BaseStats> partitionStats = new HashMap<PartitionKey, BaseStats>();

	@Override
	public synchronized void onFilesCreated(PartitionKey key, long startOffset,
			String resolvedFolder, Collection<String> eventTypes,
			String tmpFileName) {
		BaseStats stats = getStats(key);
		stats.cleanup();
		for (String eventType : eventTypes) {
			stats.getEventCounts().put(eventType, 0L);
			stats.getErrorCounts().put(eventType, 0L);
		}
		stats.setLoadStartTime(System.currentTimeMillis());
	}

	@Override
	public void onBatchBegin(PartitionKey key, long headOffset) {
	}

	@Override
	public void onEventWritten(PartitionKey key, String eventType,
			JetstreamEvent event) {
		BaseStats stats = getStats(key);
		stats.incEventCount(eventType, 1);
	}

	@Override
	public void onEventError(PartitionKey key, String eventType,
			JetstreamEvent event) {
		BaseStats stats = getStats(key);
		stats.incErrorCount(eventType, 1);
	}

	@Override
	public void onBatchEnd(PartitionKey key, long tailOffset) {
	}

	@Override
	public synchronized boolean onFilesCommited(PartitionKey key,
			long startOffset, long endOffset, String folder, String destFileName) {
		BaseStats stats = getStats(key);
		stats.setLoadEndTime(System.currentTimeMillis());
		return commitStats(key, startOffset, endOffset, folder, destFileName,
				stats);
	}

	@Override
	public synchronized void onFilesDropped(PartitionKey key, String folder) {
		getStats(key).cleanup();
	}

	protected BaseStats getStats(PartitionKey key) {
		BaseStats ret = partitionStats.get(key);
		if (ret == null) {
			ret = createStast();
			partitionStats.put(key, ret);
		}

		return ret;
	}

	protected BaseStats createStast() {
		return new BaseStats();
	}

	protected abstract boolean commitStats(PartitionKey key, long startOffset,
			long endOffset, String folder, String destFileName, BaseStats stats);

	public static class BaseStats {
		private final Map<String, Long> eventCounts = new LinkedHashMap<String, Long>();
		private final Map<String, Long> errorCounts = new LinkedHashMap<String, Long>();
		private long loadStartTime = Long.MAX_VALUE;
		private long loadEndTime = 0L;

		public Map<String, Long> getEventCounts() {
			return eventCounts;
		}

		public void incEventCount(String eventType, long delta) {
			Long v = eventCounts.get(eventType);
			if (v == null) {
				return;
			}
			v += delta;
			eventCounts.put(eventType, v);
		}

		public Map<String, Long> getErrorCounts() {
			return errorCounts;
		}

		public void incErrorCount(String eventType, long delta) {
			Long v = errorCounts.get(eventType);
			if (v == null) {
				return;
			}
			v += delta;
			errorCounts.put(eventType, v);
		}

		public long getLoadStartTime() {
			return loadStartTime;
		}

		public void setLoadStartTime(long loadStartTime) {
			this.loadStartTime = loadStartTime;
		}

		public long getLoadEndTime() {
			return loadEndTime;
		}

		public void setLoadEndTime(long loadEndTime) {
			this.loadEndTime = loadEndTime;
		}

		public void cleanup() {
			eventCounts.clear();
			errorCounts.clear();
			loadStartTime = Long.MAX_VALUE;
			loadEndTime = 0L;
		}
	}
}
