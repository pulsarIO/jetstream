/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.util.Collection;

import com.ebay.jetstream.event.JetstreamEvent;

/**
 * @author weifang
 * 
 */
public interface BatchListener {
	void onFileCreated(PartitionKey key, long startOffset, String folder,
			String tmpFileName);

	void onBatchCompleted(PartitionKey key, long eventWrittenCount,
			long eventErrorCount, long headOffset,
			Collection<JetstreamEvent> events);

	boolean onFileCommited(PartitionKey key, long startOffset, long endOffset,
			String folder, String destFileName);

	void onFileDropped(PartitionKey key, String folder, String tmpFileName);
}
