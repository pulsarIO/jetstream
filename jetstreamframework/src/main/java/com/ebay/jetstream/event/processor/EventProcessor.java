/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.event.processor;

import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.event.EventSource;
import com.ebay.jetstream.event.Monitorable;
import com.ebay.jetstream.event.MonitorableStatCollector;

/**
 * Event Processors are both EventSources and EventSinks, and do some kind of processing on the event stream. They are
 * different than EventChannels in that they are not tied to external resources.
 * 
 * 
 */
public interface EventProcessor extends EventSource, EventSink, Monitorable, MonitorableStatCollector {
}
