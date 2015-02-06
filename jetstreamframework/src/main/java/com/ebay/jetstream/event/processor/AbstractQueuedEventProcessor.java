/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamErrorCodes;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.event.support.channel.PipelineFlowControl;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.util.RequestQueueProcessor;
import com.ebay.jetstream.xmlser.Hidden;

public abstract class AbstractQueuedEventProcessor extends AbstractEventProcessor {
	
	private enum ActivityState {PAUSED, STOPPED, RUNNING, PREPARED, UNINITIALIZED}
    private RequestQueueProcessor m_workQueue;
    private int m_nMaxWorkQueueSize = 60000;  // default 100k
    private int m_nResumeThreshold;
	private int m_nNumWorkerThreads = 1; // default 1;	
	private QueuedEventProcessorConfiguration m_configuration;
	
	private AtomicLong m_highWater = new AtomicLong();
	private volatile ActivityState m_state = ActivityState.UNINITIALIZED;
	private final LongEWMACounter m_avgLatency = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
	
	private final PipelineFlowControl m_flowHandler = new PipelineFlowControl(this);
	Logger logger = LoggerFactory.getLogger("com.ebay.jetstream.event.processor.esper");
	
	
	@Override
	public void afterPropertiesSet() throws Exception {
		m_nResumeThreshold = m_nMaxWorkQueueSize / 20; // 5%
		prepare();
	}
	
	public long getAverageLatencyFromSource() {
		return m_avgLatency.get();
	}
	
	public int getEventQueueHighWaterMark() {
		return m_highWater.intValue();
	}
	
	public int getMaxWorkQueueSize() {
		return m_nMaxWorkQueueSize;
	}
	
	public int getNumWorkerThreads() {
		return m_nNumWorkerThreads;
	}
	
	public long getQueuedEventCount() {
		return m_workQueue != null ? m_workQueue.getPendingRequests() : 0;
	}
	
	public int getResumeThreshold() {
		return m_nResumeThreshold;
	}

	public String getState() {
		return m_state.toString();
	}

	@Override
	@ManagedOperation 
	public void pause() {
		pausePublisher("pauseEvent received from EventSink");
	}
	
	@Override
	@ManagedOperation
	public void resume() {
		resumePublisher("ResumeEvent received from EventSink");
	}
	
	public void setAdviceListener(Advice adviceListener) {
		m_adviceListener = adviceListener;
	}

	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		m_flowHandler.setApplicationEventPublisher(applicationEventPublisher);
	}

	@Required
	public <C extends QueuedEventProcessorConfiguration> void setConfiguration(C config) {
		m_configuration = config;
		m_nNumWorkerThreads = m_configuration.getThreadPoolSize();
		m_nMaxWorkQueueSize = m_configuration.getQueueSizeLimit();
	}


	public void shutDown() {
	
		pausePublisher("Application getting gracefulShutdown");
		if (m_workQueue != null) {
			while (m_workQueue.getPendingRequests() != 0) {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					logger.error( e.getLocalizedMessage(), e);
				}
			}
		}
		
		stop(true);
		logger.warn( getBeanName() + " Shutdown has been completed");
	}

	@ManagedOperation
	public void stop() {
		stop(true);
	}

	protected abstract String getComponentName();

	@SuppressWarnings("unchecked")
	@Hidden
	protected <C extends QueuedEventProcessorConfiguration> C getConfiguration() {
		return (C)m_configuration;
	}

	protected abstract EventProcessRequest getProcessEventRequest(JetstreamEvent event);

	protected abstract void init();
	
	protected synchronized void pausePublisher(String cause) {
		if (m_state != ActivityState.PAUSED) {
			m_flowHandler.pause();
			m_state = ActivityState.PAUSED;
			incrementPauseCounter();
			logger.warn( "Processor " + getBeanName() + " paused: " + cause);
		}
	}
	
	protected void postEventProcessed(JetstreamEvent event) {
		fireSendEvent(event);
		incrementEventSentCounter();
		if (m_state == ActivityState.PAUSED && m_workQueue.getPendingRequests() < m_nResumeThreshold)
			resume();
	}
	
	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextRefreshedEvent)
			start();
	}
	
	protected void queueEvent(EventProcessRequest eventRequest) throws EventException {
		
		long nQueueSize = m_workQueue.getPendingRequests();
		boolean bQueued = nQueueSize < getMaxWorkQueueSize();
		if (bQueued) {
			bQueued = m_workQueue.processRequest(eventRequest);
			if (bQueued && ++nQueueSize > m_highWater.get())
				m_highWater.set(nQueueSize);
		}
		
		if (!bQueued) {
			if (logger.isDebugEnabled())
				logger.debug( "Failed to offer event to queue, current size: " + m_workQueue.getPendingRequests());
			
			JetstreamEvent event = eventRequest.getEvent();
			if (event instanceof ShutdownJetstreamEvent == false) {
				// This check is mandatory. Or else we will ending up retrying a shutdhown jetstream event
				if (getAdviceListener() != null)
					sendToAdviceListener(event, RetryEventCode.QUEUE_FULL, " Queue is full");
				else {
					incrementEventDroppedCounter();
					pausePublisher(getBeanName() + " has reached upper limit " + m_workQueue.getPendingRequests()
							+ " and tried to pause publisher.");
				}
			}
		}
	}
    
	protected void queueEvent(JetstreamEvent event) throws EventException {
		queueEvent(getProcessEventRequest(event));
	}

	protected synchronized void resumePublisher(String cause) {
		if (m_state != ActivityState.RUNNING) { // Bug 6653 fix
			m_flowHandler.resume();
			m_state = ActivityState.RUNNING;
			incrementResumeCounter();
			logger.warn( "Processor " + getBeanName() + " resumed: " + cause);
		}
	}
	
	@Override
	public void sendEvent(JetstreamEvent event) throws EventException {
		switch (m_state) {
			case PAUSED:
				break;
			case STOPPED: {
				logger.error( "Processor" + getBeanName() + "has been paused or stopped. Cannot accept the event: ", getComponentName());
				throw new EventException("Processor "  + getBeanName() + " is paused or stopped. Can not take in any more events.",
					isPauseIfAnyEventSinkPauses() ? JetstreamErrorCodes.EVENT_SINK_PAUSED.toString()
							: JetstreamErrorCodes.EVENT_SINKS_PAUSED.toString());
			}
			case UNINITIALIZED:
				prepare();
			case PREPARED:
				start();
			case RUNNING: {
				incrementEventRecievedCounter();
				queueEvent(event);
				Long lngOrigin = (Long)event.get(JetstreamReservedKeys.EventTime.toString());
				if (lngOrigin != null) {
					long lNow = System.currentTimeMillis();
					long lOrigin = lngOrigin.longValue();
					if (lNow >= lOrigin)
						m_avgLatency.add(lNow - lOrigin + (lOrigin % 2));
					else
						logger.warn( "Event origin timestamp is in the future (" + 
								lOrigin + ", " + lNow + ")");
				}
			}
			default: break;
		}
	}
	
	@ManagedOperation
	protected void start() {
		start(true);
	}

	protected void stop(boolean bHardStop) {
	
		pausePublisher(bHardStop ? "Paused by stop call" : "Received Pause from sink");
		try {
			m_state = ActivityState.STOPPED;
			// More stringent test than isStarted(), in case events lost && shutdown incomplete
			m_workQueue.shutdown();
		}
		catch (Throwable e) {
			logger.error( e.getLocalizedMessage(), e );
		}
	}

	private synchronized void prepare() {
		if (m_state == ActivityState.UNINITIALIZED || m_state == ActivityState.STOPPED) {
			m_state = ActivityState.PREPARED;
	
			if (m_workQueue == null) {
				m_workQueue = new RequestQueueProcessor(getMaxWorkQueueSize(), getNumWorkerThreads(), getBeanName());
			}
				
			init();
		}
	}

	private void sendToAdviceListener(JetstreamEvent event, RetryEventCode code, String errorMsg) {
		if (getAdviceListener() != null)
			getAdviceListener().retry(event, code, errorMsg);
	}
	
	private void start(boolean isExplicitCall) {

		// we need to call this BEFORE checking whether we are in sync mode
		prepare();

		if (m_state != ActivityState.RUNNING) {
		
			if (m_state == ActivityState.PREPARED) {
				m_state = ActivityState.RUNNING;
			
				if (logger.isInfoEnabled()) {
					logger.info( "Successfully started \'" + getBeanName() + "\' " + getBeanName());
							
				}
			}
			resumePublisher("Resumed by start call");
		}
	}
}
