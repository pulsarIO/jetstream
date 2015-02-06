/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import com.ebay.jetstream.event.BatchEventSink;
import com.ebay.jetstream.event.BatchEventSource;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.EventMetaInfo;
import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.event.JetstreamErrorCodes;
import com.ebay.jetstream.event.JetstreamEvent;

/**
 * @author xiaojuwu1
 */
public abstract class AbstractBatchEventSource extends AbstractEventSource
		implements BatchEventSource {

	private final Collection<BatchEventSink> m_batchSinks = new CopyOnWriteArrayList<BatchEventSink>();

	protected void fireSendEvents(Collection<JetstreamEvent> events,
			EventMetaInfo meta)  throws EventException {
		if (m_batchSinks.isEmpty()) {
			throw new EventException("All Event Sinks are paused",
					JetstreamErrorCodes.EVENT_SINKS_PAUSED.toString());
		}
		for (BatchEventSink b : m_batchSinks) {
			b.sendEvents(events, meta);
		}
	}

	@Override
	public void addBatchEventSink(BatchEventSink target) {
		m_batchSinks.add(target);
	}

	@Override
	public Collection<BatchEventSink> getBatchEventSinks() {
		return m_batchSinks;
	}

	@Override
	public void removeBatchEventSink(BatchEventSink target) {
		m_batchSinks.remove(target);
	}

	@Override
	public void setBatchEventSinks(Collection<BatchEventSink> sinks) {
		if (sinks != null && !sinks.isEmpty()) {
			m_batchSinks.clear();
			m_batchSinks.addAll(sinks);
		}
	}

	@Override
	public void addEventSink(EventSink sink) {
		if (sink instanceof BatchEventSink) {
			addBatchEventSink((BatchEventSink) sink);
		} else {
			super.addEventSink(sink);
		}
	}

	@Override
	public void setEventSinks(Collection<EventSink> sinks) {
		for (EventSink s : sinks) {
			if (s instanceof BatchEventSink) {
				addBatchEventSink((BatchEventSink) s);
			} else {
				addEventSink(s);
			}
		}
	}

}
