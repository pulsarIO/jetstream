/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
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
