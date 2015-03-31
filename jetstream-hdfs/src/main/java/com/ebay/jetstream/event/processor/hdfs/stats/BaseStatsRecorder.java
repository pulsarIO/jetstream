/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.stats;

import java.util.Collection;
import java.util.HashMap;
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
	public synchronized void onFileCreated(PartitionKey key, long startOffset,
			String folder, String tmpFileName) {
		BaseStats stats = getStats(key);
		stats.cleanup();
		stats.setLoadStartTime(System.currentTimeMillis());
	}

	@Override
	public void onBatchCompleted(PartitionKey key, long eventWrittenCount,
			long eventErrorCount, long headOffset,
			Collection<JetstreamEvent> events) {
		BaseStats stats = getStats(key);
		stats.incEventCount(eventWrittenCount);
		stats.incErrorCount(eventErrorCount);
	}

	@Override
	public synchronized boolean onFileCommited(PartitionKey key,
			long startOffset, long endOffset, String folder, String destFileName) {
		BaseStats stats = getStats(key);
		stats.setLoadEndTime(System.currentTimeMillis());
		return commitStats(key, startOffset, endOffset, folder, destFileName,
				stats);
	}

	@Override
	public synchronized void onFileDropped(PartitionKey key, String folder,
			String tmpFileName) {
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
		private long eventCount = 0L;
		private long errorCount = 0L;
		private long loadStartTime = Long.MAX_VALUE;
		private long loadEndTime = 0L;

		public long getEventCount() {
			return eventCount;
		}

		public void setEventCount(long eventCount) {
			this.eventCount = eventCount;
		}

		public void incEventCount(long delta) {
			eventCount += delta;
		}

		public long getErrorCount() {
			return errorCount;
		}

		public void setErrorCount(long errorCount) {
			this.errorCount = errorCount;
		}

		public void incErrorCount(long delta) {
			errorCount += delta;
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
			eventCount = 0L;
			errorCount = 0L;
			loadStartTime = Long.MAX_VALUE;
			loadEndTime = 0L;
		}
	}
}
