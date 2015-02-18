/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.notification.AlertListener;
import com.ebay.jetstream.notification.AlertListener.AlertStrength;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.ebay.jetstream.xmlser.Hidden;

public abstract class AbstractOutboundChannel extends AbstractNamedBean
		implements OutboundChannel, BeanChangeAware, InitializingBean,
		ApplicationListener, ShutDownable {

	protected final LongCounter m_alarmsRaisedCounter = new LongCounter();
	protected final LongCounter m_alarmsClearedCounter = new LongCounter();

		
	protected final LongCounter m_totalEventsSent = new LongCounter();
	protected final LongCounter m_totalEventsDropped = new LongCounter();
	protected final LongCounter m_totalEventsReceived = new LongCounter();

	protected LongEWMACounter m_eventsReceivedPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());

	protected LongEWMACounter m_eventsSentPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());

	protected final AtomicBoolean m_alarmRaised = new AtomicBoolean(false);

	JetstreamEvent m_lastEvent;

	private ChannelAlarmListener m_alarmListener;
	
	private AlertListener m_alertListener;
	
	private Advice m_adviceListener;
	
	@Hidden
	public Advice getAdviceListener() {
		return m_adviceListener;
	}

	public void setAdviceListener(Advice adviceListener) {
		this.m_adviceListener = adviceListener;
	}
	
	@Hidden
	public AlertListener getAlertListener() {
		return m_alertListener;
	}

	public void setAlertListener(AlertListener alertListner) {
		this.m_alertListener = alertListner;
	}
	

	/**
	 * @return the messagesReceived
	 */
	@Override
	public long getTotalEventsReceived() {
		return m_totalEventsReceived.get();
	}

	
	/* (non-Javadoc)
	 * @see com.ebay.jetstream.event.Monitorable#getTotalEventsSent()
	 */
	@Override
	public long getTotalEventsSent() {
		return m_totalEventsSent.get();
	}

	/* (non-Javadoc)
	 * @see com.ebay.jetstream.event.Monitorable#getTotalEventsDropped()
	 */
	@Override
	public long getTotalEventsDropped() {
		return m_totalEventsDropped.get();
	}

	/* (non-Javadoc)
	 * @see com.ebay.jetstream.event.Monitorable#getEventsReceivedPerSec()
	 */
	@Override
	public long getEventsReceivedPerSec() {
		return m_eventsReceivedPerSec.get();
	}

	/* (non-Javadoc)
	 * @see com.ebay.jetstream.event.Monitorable#getEventsSentPerSec()
	 */
	@Override
	public long getEventsSentPerSec() {
		return m_eventsSentPerSec.get();
	}

	/**
	 * @return
	 */
	public long getAlarmsRaisedCounter() {
		return m_alarmsRaisedCounter.get();
	}

	/**
	 * @return
	 */
	public long getAlarmsClearedCounter() {
		return m_alarmsClearedCounter.get();
	}

	/**
	   * 
	   */
	@ManagedOperation
	public void resetStats() {
		m_totalEventsReceived.reset();
		m_eventsReceivedPerSec.reset();
		m_totalEventsSent.reset();
		m_eventsSentPerSec.reset();
		m_totalEventsDropped.reset();
		m_alarmsRaisedCounter.reset();
		m_alarmsClearedCounter.reset();
	}

	/**
	 * 
	 */
	protected void incrementEventDroppedCounter() {
		m_totalEventsDropped.increment();
	}

	/**
	 * 
	 */
	protected void incrementEventDroppedCounter(long lNumber) {
		m_totalEventsDropped.addAndGet(lNumber);
	}
	
	/**
	 * 
	 */
	protected void incrementEventRecievedCounter() {
		m_totalEventsReceived.increment();
		m_eventsReceivedPerSec.increment();
	}

	/**
	 * 
	 */
	protected void incrementEventSentCounter() {
		m_totalEventsSent.increment();
		m_eventsSentPerSec.increment();
	}

	/**
	 * @param lNumber
	 */
	protected void incrementEventSentCounter(long lNumber) {
		m_totalEventsSent.addAndGet(lNumber);
		m_eventsSentPerSec.add(lNumber);
	}

	
	/**
	 * 
	 */
	protected void incrementAlarmsRaisedCounter() {
		m_alarmsRaisedCounter.increment();
	}

	/**
	 * 
	 */
	protected void incrementAlarmsClearedCounter() {
		m_alarmsClearedCounter.increment();
	}

	/**
	 * @return
	 */
	protected boolean getAlarmRaisedState() {
		return m_alarmRaised.get();
	}

	/**
	 * @param state
	 */
	protected void setAlarmRaisedState(boolean state) {
		m_alarmRaised.set(state);
	}

	/**
	 * @param alarmClearedCount
	 *            the alarmClearedCount to set
	 */
	@ManagedOperation
	public void resetAlarmsCleared() {
		m_alarmsClearedCounter.set(0);
	}

	/**
	 * @param alarmRaisedCount
	 *            the alarmRaisedCount to set
	 */
	@ManagedOperation
	public void resetAlarmsRaised() {
		m_alarmsRaisedCounter.set(0);
	}

	/**
	 * @param eventsDropped
	 *            the eventsDropped to set
	 */
	@ManagedOperation
	public void resetEventsDropped() {
		m_totalEventsDropped.set(0);
	}

	/**
	 * @param messagesSent
	 *            the messagesSent to set
	 */
	@ManagedOperation
	public void resetEventsSent() {
		m_totalEventsSent.set(0);
	}

	/**
	 * @param event
	 */
	protected void setLastEvent(JetstreamEvent event) {
		m_lastEvent = event;

	}

	/**
	 * @return
	 */
	public JetstreamEvent getLastEvent() {
		return m_lastEvent;

	}

	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	
	public void onApplicationEvent(ApplicationEvent event) {

		processApplicationEvent(event); // we do it this way so it is consistent
										// with AbstractInboundChannel

	}
    
	
	/* (non-Javadoc)
	 * @see com.ebay.jetstream.event.channel.OutboundChannel#getAlarmListener()
	 */
	@Override
	public ChannelAlarmListener getAlarmListener() {
		return m_alarmListener;
	}

	/* (non-Javadoc)
	 * @see com.ebay.jetstream.event.channel.OutboundChannel#setAlarmListener(com.ebay.jetstream.event.channel.ChannelAlarmListener)
	 */
	@Override
	public void setAlarmListener(ChannelAlarmListener alarmListener) {
		m_alarmListener = alarmListener;

	}

	/**
	 * @param event
	 */
	public abstract void processApplicationEvent(ApplicationEvent event);
	
	public void close() throws EventException {
		m_eventsReceivedPerSec.destroy();
		m_eventsSentPerSec.destroy();
	}
	
	public void open() throws EventException {

		m_eventsReceivedPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());

		m_eventsSentPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());

	}
	
	@Override
	public long getTotalPauseCount() {
		
		return 0;
	}

	@Override
	public long getTotalResumeCount() {
		
		return 0;
	}
	
	protected void postAlert(String msg, AlertStrength strength) {
		if (m_alertListener != null) {
			m_alertListener.sendAlert(this.getBeanName(), msg, strength);
		}
	}
}
