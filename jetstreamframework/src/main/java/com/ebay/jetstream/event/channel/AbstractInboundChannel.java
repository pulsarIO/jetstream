/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationListener;
import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.support.AbstractEventSource;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.notification.AlertListener;
import com.ebay.jetstream.notification.AlertListener.AlertStrength;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @authors Raji Muthupandian (rmuthupandian@ebay.com & shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 * All channel implementations must extend this class and implement all the abstract methods.
 */

public abstract class AbstractInboundChannel extends AbstractEventSource
		implements InboundChannel, BeanChangeAware, InitializingBean,
		ApplicationListener, XSerializable, ShutDownable {
	
	protected enum ChannelOperationState {PAUSE, RESUME}

	protected final AtomicBoolean m_isPaused = new AtomicBoolean(false);

	protected final LongCounter m_totalEventsReceived = new LongCounter();

	protected final LongCounter m_pauseCount = new LongCounter();

	protected final LongCounter m_resumeCount = new LongCounter();

	protected final LongCounter m_totalEventsSent = new LongCounter();

	protected final LongCounter m_totalEventsDropped = new LongCounter();

	protected final LongCounter m_eventSentToAdviceListener = new LongCounter();

	protected LongEWMACounter m_eventsReceivedPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());

	protected LongEWMACounter m_eventsSentPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());

	protected final AtomicBoolean m_shutdownStatus = new AtomicBoolean(false);

	private JetstreamEvent m_lastEvent;
	
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
	 * 
	 * @return
	 */
	protected long getEventSentToAdviceListener() {
		return m_eventSentToAdviceListener.get();
	}

	/**
	 * @return the messagesReceived
	 */
	public long getTotalEventsReceived() {
		return m_totalEventsReceived.get();
	}

	/**
	 * @return the pauseCount
	 */
	public long getTotalPauseCount() {
		return m_pauseCount.get();
	}

	/**
	 * @return the resumeCount
	 */
	public long getTotalResumeCount() {
		return m_resumeCount.get();
	}

	/**
	 * @return
	 */
	protected boolean getShutdownStatus() {
		return m_shutdownStatus.get();
	}

	/**
	 * 
	 */
	protected void incrementAdviceListenerCount() {
		m_eventSentToAdviceListener.increment();
	}

	/**
	 * @param messagesReceived
	 *            the messagesReceived to set
	 */
	@ManagedOperation
	protected void resetEventsReceived() {
		m_totalEventsReceived.set(0);
	}

	/**
	 * @param pauseCount
	 *            the pauseCount to set
	 */
	@ManagedOperation
	protected void resetPauseCount() {
		m_pauseCount.set(0);
	}

	/**
	 * @param resumeCount
	 *            the resumeCount to set
	 */
	@ManagedOperation
	protected void resetResumeCount() {
		m_resumeCount.set(0);
	}

	@Override
	public long getTotalEventsSent() {
		return m_totalEventsSent.get();
	}

	@Override
	public long getTotalEventsDropped() {
		return m_totalEventsDropped.get();
	}

	@Override
	public long getEventsReceivedPerSec() {
		return m_eventsReceivedPerSec.get();
	}

	@Override
	public long getEventsSentPerSec() {
		return m_eventsSentPerSec.get();
	}

	/**
	   * 
	   */
	@ManagedOperation
	public void resetStats() {
		m_totalEventsReceived.reset();
		m_eventsReceivedPerSec.reset();
		m_pauseCount.reset();
		m_resumeCount.reset();
		m_totalEventsSent.reset();
		m_eventsSentPerSec.reset();
		m_totalEventsDropped.reset();
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
	 * 
	 */
	protected void incrementPauseCounter() {
		m_pauseCount.increment();
	}

	/**
	 * 
	 */
	protected void incrementResumeCounter() {
		m_resumeCount.increment();
	}

	/**
	 * @return
	 */
	
	protected boolean isPaused() {
		return m_isPaused.get();
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
	
	protected void changeState(ChannelOperationState state) {
		switch(state) {
		case PAUSE:
			if (!m_isPaused.get()) {
				m_isPaused.set(true);
				incrementPauseCounter();
			}
			break;
		case RESUME:
			if (m_isPaused.get()) {
				m_isPaused.set(false);
				incrementResumeCounter();
			}
			break;
				
		}
	}
	
	public void open() throws EventException {
		
		m_eventsReceivedPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
		m_eventsSentPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());

	}
	
	public void close() throws EventException {
		m_eventsReceivedPerSec.destroy();
		m_eventsSentPerSec.destroy();
	}
	
	protected void postAlert(String msg, AlertStrength strength) {
		if (m_alertListener != null) {
			m_alertListener.sendAlert(this.getBeanName(), msg, strength);
		}
	}
	
}
