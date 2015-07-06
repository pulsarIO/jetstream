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
	void onFilesCreated(PartitionKey key, long startOffset,
			String resolvedFolder, Collection<String> eventTypes,
			String tmpFileName);

	void onBatchBegin(PartitionKey key, long headOffset);

	void onEventWritten(PartitionKey key, String eventTypeCategory,
			JetstreamEvent event);

	void onEventError(PartitionKey key, String eventTypeCategory,
			JetstreamEvent event);

	void onBatchEnd(PartitionKey key, long tailOffset);

	boolean onFilesCommited(PartitionKey key, long startOffset, long endOffset,
			String resolvedFolder, String destFileName);

	void onFilesDropped(PartitionKey key, String resolvedFolder);
}
