/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.BatchSource;
import com.ebay.jetstream.event.BatchSourceCommand;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.EventMetaInfo;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.processor.BatchEventProcessor;
import com.ebay.jetstream.event.support.channel.PipelineFlowControl;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.notification.AlertListener;
import com.ebay.jetstream.notification.AlertListener.AlertStrength;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.ebay.jetstream.xmlser.Hidden;

/**
 * @author weifang, xiaojuwu1
 * 
 */
public abstract class AbstractBatchEventProcessor extends
		AbstractBatchEventSource implements BatchEventProcessor,
		ApplicationListener, ApplicationEventPublisherAware, InitializingBean,
		BeanNameAware, BeanChangeAware, ShutDownable {

	protected enum ProcessorOperationState {
		PAUSE, RESUME
	}

	protected final AtomicBoolean m_isPaused = new AtomicBoolean(false);

	private final LongCounter m_totalEventsReceived = new LongCounter();
	private final LongEWMACounter m_eventsReceivedPerSec = new LongEWMACounter(
			60, MessageServiceTimer.sInstance().getTimer());
	private final LongCounter m_totalEventsSent = new LongCounter();
	private final LongEWMACounter m_eventsSentPerSec = new LongEWMACounter(60,
			MessageServiceTimer.sInstance().getTimer());
	private final LongCounter m_totalEventsDropped = new LongCounter();
	private final LongCounter m_pauseCount = new LongCounter();
	private final LongCounter m_resumeCount = new LongCounter();
	private final PipelineFlowControl m_pipelineflowcontroller = new PipelineFlowControl(
			this);
	private AlertListener m_alertListener;
	protected Advice m_adviceListener = null;

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

	public void setAlertListener(AlertListener alertListener) {
		this.m_alertListener = alertListener;
	}

	public PipelineFlowControl getPipelineflowcontroller() {
		return m_pipelineflowcontroller;
	}

	@Override
	public long getEventsReceivedPerSec() {
		return m_eventsReceivedPerSec.get();
	}

	@Override
	public long getEventsSentPerSec() {
		return m_eventsSentPerSec.get();
	}

	@Override
	public long getTotalEventsDropped() {
		return m_totalEventsDropped.get();
	}

	@Override
	public long getTotalEventsReceived() {
		return m_totalEventsReceived.get();
	}

	@Override
	public long getTotalEventsSent() {
		return m_totalEventsSent.get();
	}

	@Override
	public long getTotalPauseCount() {
		return m_pauseCount.get();
	}

	@Override
	public long getTotalResumeCount() {
		return m_resumeCount.get();
	}

	public void incrementEventDroppedCounter() {
		m_totalEventsDropped.increment();
	}

	public void incrementEventDroppedCounter(long lNumber) {
		m_totalEventsDropped.addAndGet(lNumber);
	}

	public void incrementEventRecievedCounter() {
		m_totalEventsReceived.increment();
		m_eventsReceivedPerSec.increment();
	}

	public void incrementEventRecievedCounter(long lNumber) {
		m_totalEventsReceived.addAndGet(lNumber);
		m_eventsReceivedPerSec.add(lNumber);
	}

	public void incrementEventSentCounter() {
		m_totalEventsSent.increment();
		m_eventsSentPerSec.increment();
	}

	public void incrementEventSentCounter(long lNumber) {
		m_totalEventsSent.addAndGet(lNumber);
		m_eventsSentPerSec.add(lNumber);
	}

	public void incrementPauseCounter() {
		m_pauseCount.increment();
	}

	public void incrementResumeCounter() {
		m_resumeCount.increment();
	}

	protected void resetCounters() {
		m_totalEventsReceived.reset();
		m_eventsReceivedPerSec.reset();
		m_totalEventsSent.reset();
		m_eventsSentPerSec.reset();
		m_totalEventsDropped.reset();
	}

	@Override
	public void setApplicationEventPublisher(
			ApplicationEventPublisher applicationEventPublisher) {
		m_pipelineflowcontroller
				.setApplicationEventPublisher(applicationEventPublisher);

	}

	protected void changeState(ProcessorOperationState state) {
		switch (state) {
		case PAUSE:
			if (!m_isPaused.get()) {
				m_isPaused.set(true);
				incrementPauseCounter();
				getPipelineflowcontroller().pause();
			}
			break;
		case RESUME:
			if (m_isPaused.get()) {
				m_isPaused.set(false);
				incrementResumeCounter();
				getPipelineflowcontroller().resume();
			}
			break;

		}
	}

	protected boolean isPaused() {
		return m_isPaused.get();
	}

	protected void postAlert(String msg, AlertStrength strength) {
		if (m_alertListener != null) {
			m_alertListener.sendAlert(this.getBeanName(), msg, strength);
		}
	}

	@Override
	public void sendEvents(Collection<JetstreamEvent> events, EventMetaInfo meta)
			throws EventException {
		BatchSourceCommand action = meta.getAction();
		BatchResponse res = null;
		switch (action) {
		case OnNextBatch:
			res = onNextBatch(meta.getBatchSource(), events);
			break;
		case OnBatchProcessed:
			onBatchProcessed(meta.getBatchSource());
			break;
		case OnException:
			res = onException(meta.getBatchSource(), meta.getException());
			break;
		case OnIdle:
			res = onIdle(meta.getBatchSource());
			break;
		case OnStreamTermination:
			res = onStreamTermination(meta.getBatchSource());
			break;
		default:
			throw new IllegalArgumentException("Illegal batch sink action.");
		}
		meta.setBatchResponse(res);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.EventSink#sendEvent(com.ebay.jetstream.event
	 * .JetstreamEvent)
	 */
	@Override
	public void sendEvent(JetstreamEvent event) throws EventException {
		throw new EventException("Unsupported operation");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.common.ShutDownable#getPendingEvents()
	 */
	@Override
	public int getPendingEvents() {
		return 0;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Management.addBean(getBeanName(), this);
		init();
	}

	public abstract void init() throws Exception;

	public abstract BatchResponse onNextBatch(BatchSource source,
			Collection<JetstreamEvent> events) throws EventException;
	
	public abstract void onBatchProcessed(BatchSource source);

	public abstract BatchResponse onStreamTermination(BatchSource source)
			throws EventException;

	public abstract BatchResponse onException(BatchSource source, Exception ex);

	public abstract BatchResponse onIdle(BatchSource source);

}
