/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;


import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.AbstractOutboundChannel;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageServiceTimer;

@ManagedResource(objectName = "Event/Channel", description = "Write Jetstream data to disk")
public abstract class OutboundRollingFileChannel extends AbstractOutboundChannel {
	
	private boolean m_stop = false;
	private boolean m_pause = false;
	private boolean m_restart = false;
	private String streamType = "";
	private RollingFileAppender rollingFileAppender = null;
	private static final Logger LOGGER = Logger.getLogger("com.ebay.jetstream.event.channel.file");
	
	public OutboundRollingFileChannel(String streamType, String fileName, int backups, String fileSize) throws IOException {
		this.streamType = streamType;
		rollingFileAppender = new RollingFileAppender(new EventLayout(), fileName);
		rollingFileAppender.setMaxBackupIndex(backups);
		rollingFileAppender.setMaxFileSize(fileSize);
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {	
		Management.addBean(getBeanName(), this);
	}

	@Override
  	public void processApplicationEvent(ApplicationEvent event) { }
	
	@Override
	public void flush() throws EventException {	}
	
	public void open() throws EventException {
		LOGGER.info(  "Opening Outbound File Channel");
  	    m_eventsReceivedPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
  		m_eventsSentPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
	}
	
	@ManagedOperation
	public void resetStats() {
		super.resetStats();
	}
	
	abstract String createEventLine(JetstreamEvent e);
	
	/*** this is the point where the data get into this class */
	public void sendEvent(JetstreamEvent event) throws EventException {
		if (m_stop || m_pause) {
			return;
		}
 		if (event == null) {
			return;
		}
		incrementEventRecievedCounter();
		
		String streamType = event.getEventType();
		
		if (!this.streamType.equals(streamType)) {
			incrementEventDroppedCounter();
			return;
		}
		
		JetstreamEvent userEvent = new JetstreamEvent();
		userEvent.putAll(event);
		
		/** empty event. drop it */
		if (userEvent.size() == 0) {
			LOGGER.info(   "the event is empty. Should not happen?");
			incrementEventDroppedCounter();
			return;
		}
			
		String eventLineString = createEventLine(userEvent);
		rollingFileAppender.doAppend(new LoggingEvent("", Logger.getLogger("whocares"), Level.INFO, eventLineString, null));
		incrementEventSentCounter();
	}

	public void shutDown() {
		close();
	}

	@Override
	public String toString() {
		return getBeanName();
	}
	
	@Override
	public int getPendingEvents() {
		return 0;
	}
	
	@ManagedOperation
	public void pause() {
		m_pause = true;
	}
	
	@ManagedOperation
	public void resume() {
		m_pause = false;
	}
	
}

