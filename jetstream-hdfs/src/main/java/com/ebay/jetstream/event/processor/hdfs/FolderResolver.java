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
public interface FolderResolver {
	String getCurrentFolder(Collection<JetstreamEvent> thisBatch);

	boolean shouldMoveToNext(Collection<JetstreamEvent> lastBatch,
			String currentFolder);
}
