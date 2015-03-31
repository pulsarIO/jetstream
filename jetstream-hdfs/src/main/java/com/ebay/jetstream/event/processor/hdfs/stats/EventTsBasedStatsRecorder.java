/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.stats;

import java.util.Collection;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.PartitionKey;

/**
 * @author weifang
 * 
 */
public abstract class EventTsBasedStatsRecorder extends BaseStatsRecorder {
	@Override
	public void onBatchCompleted(PartitionKey key, long eventWrittenCount,
			long eventErrorCount, long headOffset,
			Collection<JetstreamEvent> events) {
		super.onBatchCompleted(key, eventWrittenCount, eventErrorCount,
				headOffset, events);
		EventTsBasedStats stats = (EventTsBasedStats) getStats(key);
		for (JetstreamEvent event : events) {
			Long ts = getTimestamp(event);
			if (ts != null) {
				if (stats.getMinTimestamp() > ts)
					stats.setMinTimestamp(ts);
				if (stats.getMaxTimestamp() < ts)
					stats.setMaxTimestamp(ts);
				long v = System.currentTimeMillis() - ts;
				stats.incLatency(v);
			}
		}
	}

	protected abstract Long getTimestamp(JetstreamEvent event);

	@Override
	protected BaseStats createStast() {
		return new EventTsBasedStats();
	}

	public static class EventTsBasedStats extends BaseStats {
		private long minTimestamp = Long.MAX_VALUE;
		private long maxTimestamp = 0;
		private long totalLatency = 0;

		@Override
		public void cleanup() {
			super.cleanup();
			minTimestamp = Long.MAX_VALUE;
			maxTimestamp = 0;
			totalLatency = 0;
		}

		public long getMinTimestamp() {
			return minTimestamp;
		}

		public void setMinTimestamp(long minTimestamp) {
			this.minTimestamp = minTimestamp;
		}

		public long getMaxTimestamp() {
			return maxTimestamp;
		}

		public void setMaxTimestamp(long maxTimestamp) {
			this.maxTimestamp = maxTimestamp;
		}

		public long getTotalLatency() {
			return totalLatency;
		}

		public void setTotalLatency(long totalLatency) {
			this.totalLatency = totalLatency;
		}

		public void incLatency(long latency) {
			totalLatency += latency;
		}
	}

}
