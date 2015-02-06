/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.MonitorableStatCollector;
import com.ebay.jetstream.event.support.ErrorTracker;
import com.ebay.jetstream.util.Request;

public abstract class EventProcessRequest extends Request  {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.processor");
	
	private final JetstreamEvent m_event;
	private final MonitorableStatCollector m_parent;
	
	protected EventProcessRequest(JetstreamEvent event, MonitorableStatCollector parent) {
		m_event = event;
		m_parent = parent;
	}

	public JetstreamEvent getEvent() {
		return m_event;
	}
	
	protected MonitorableStatCollector getStatCollector() {
		return m_parent;
	}
	
	@Override
	public boolean execute() {
		MonitorableStatCollector stats = getStatCollector();
		try {
			processEvent(m_event);
			afterEventProcessed(m_event, stats);
			
			return true;
		} 
		catch (Throwable t) {
			
			if (stats instanceof ErrorTracker)
				((ErrorTracker)stats).registerError(t, m_event);
			
			if (stats != null)
				stats.incrementEventDroppedCounter();
			
			LOGGER.error("failed to process event" + t.getMessage());
			
			return false;
		}
	}
	
	protected void afterEventProcessed(JetstreamEvent event, MonitorableStatCollector stats) {
		if (stats instanceof AbstractQueuedEventProcessor)
			((AbstractQueuedEventProcessor)stats).postEventProcessed(event);
		else if (stats != null)
			stats.incrementEventSentCounter();
	}
	
	protected abstract void processEvent(JetstreamEvent event) throws Exception;
}
