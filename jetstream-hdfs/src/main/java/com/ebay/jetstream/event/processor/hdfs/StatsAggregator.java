/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.util.Map;

/**
 * @author weifang
 * 
 */
public interface StatsAggregator {
	void aggregateStats(Map<String, Map<String, Object>> fileStats,
			Map<String, Object> aggregatedStats);
}
