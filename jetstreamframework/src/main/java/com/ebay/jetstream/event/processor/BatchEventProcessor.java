/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.event.processor;

import com.ebay.jetstream.event.BatchEventSink;
import com.ebay.jetstream.event.BatchEventSource;
import com.ebay.jetstream.event.Monitorable;
import com.ebay.jetstream.event.MonitorableStatCollector;

/**
 * @author xiaojuwu1
 */
public interface BatchEventProcessor extends BatchEventSource, BatchEventSink,
		Monitorable, MonitorableStatCollector {
}
