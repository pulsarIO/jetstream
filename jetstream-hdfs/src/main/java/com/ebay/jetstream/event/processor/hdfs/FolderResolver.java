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
public interface FolderResolver {
	String getCurrentFolder(Collection<JetstreamEvent> thisBatch);

	boolean shouldMoveToNext(Collection<JetstreamEvent> lastBatch,
			String currentFolder);
}
