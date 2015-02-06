/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import java.util.Collection;

/**
 * This is a tagging interface that any source that will deliver events in
 * batches and follow the batch semantics must implement.
 * 
 * @author xiaojuwu1
 * 
 */
public interface BatchEventSource {

	/**
	 * Add a batch event sink to this batch event source.
	 * 
	 * @param target
	 *            The sink that will receive the events.
	 */
	void addBatchEventSink(BatchEventSink target);

	/**
	 * Gets the complete collection of batch event sinks for this batch event source.
	 * 
	 * @return the collection of batch event sinks connected to the batch event source.
	 */
	Collection<BatchEventSink> getBatchEventSinks();

	/**
	 * Remove a batch event sink from this batch event source.
	 * 
	 * @param target
	 *            The target to remove
	 */
	void removeBatchEventSink(BatchEventSink target);

	/**
	 * Sets the complete collection of batch event sinks for this source.
	 * 
	 * @param sinks
	 *            the collection of batch event sinks to set for this source.
	 */
	void setBatchEventSinks(Collection<BatchEventSink> sinks);

}
