/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.util.List;
import java.util.Map;

/**
 * @author weifang
 * 
 */
public interface ProgressController {
	PartitionProgressController createPartitionProgressController(
			PartitionKey partitionKey);

	List<String> getSuccessCheckFolders();

	boolean isSuccess(String folder);

	Map<String, Map<String, Object>> getFileStats(String folder);

	void markSuccess(String folder, Map<String, Map<String, Object>> fileStats,
			Map<String, Object> aggregatedStats);

	void setOutputFolder(String folder);
}
