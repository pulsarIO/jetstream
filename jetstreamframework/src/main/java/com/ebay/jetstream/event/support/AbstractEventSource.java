/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NamedBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.event.EventSinkList;
import com.ebay.jetstream.event.EventSource;
import com.ebay.jetstream.event.JetstreamErrorCodes;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.channel.PauseEvent;
import com.ebay.jetstream.event.support.channel.ResumeEvent;

/**
 * @author shmurthy, xiaojuwu1
 */
public abstract class AbstractEventSource extends AbstractNamedBean implements
		EventSource, ApplicationListener, ErrorTracker {

	private boolean m_pauseIfAnyEventSinkPauses = true;
	private final AtomicBoolean m_isSourcePaused = new AtomicBoolean(false);
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.support");
	private List<EventSink> m_pausedEventSink = new CopyOnWriteArrayList<EventSink>();
	private EventSinkList m_sinkList = new EventSinkList();
	private ErrorManager m_errors = new ErrorManager();
	private final LongCounter m_cloneFailedCounter = new LongCounter();

	protected void fireSendEvent(JetstreamEvent event) throws EventException {

		if (LOGGER.isDebugEnabled())
			event.log(this);

		if (m_sinkList.getSinks().size() <= 0
				&& getPausedEventSink().size() > 0) {
			throw new EventException("All Event Sinks are  paused.....",
					JetstreamErrorCodes.EVENT_SINKS_PAUSED.toString());
		}

		int index = 0;
		int lastIndex = m_sinkList.size() - 1;

		for (EventSink sink : m_sinkList.getSinks()) {
			// if we have more than 1 sink we need to clone the event to pass to
			// another sink so they can
			// independently modify the event concurrently
			JetstreamEvent fwdEvent = null;
			if (index < lastIndex) {
				try {
					fwdEvent = event.clone();
					if (fwdEvent == null)
						m_cloneFailedCounter.increment();
				} catch (CloneNotSupportedException e) {
					m_cloneFailedCounter.increment();
					LOGGER.error(
							"unable to clone event : "
									+ e.getLocalizedMessage());

				}
			} else {
				fwdEvent = event;
			}

			index++;
			try {
				if (fwdEvent != null)
					sink.sendEvent(fwdEvent);
			} catch (EventException e) {
				if (m_pauseIfAnyEventSinkPauses) {
					throw e;
				}
			}
		}
	}

	@Override
	public void addEventSink(EventSink sink) {
		m_sinkList.addSink(sink);
	}

	@Override
	public Collection<EventSink> getEventSinks() {
		return m_sinkList;
	}

	@Override
	public void removeEventSink(EventSink sink) {
		m_sinkList.remove(sink);
	}

	@Override
	public void setEventSinks(Collection<EventSink> sinks) {
		if (sinks instanceof EventSinkList) {
			m_sinkList = (EventSinkList) sinks;
		} else {
			for (EventSink s : sinks) {
				addEventSink(s);
			}
		}
	}

	/**
	 * @return the pausedEventSink
	 */
	public List<EventSink> getPausedEventSink() {
		return Collections.unmodifiableList(m_pausedEventSink);
	}

	private boolean isEventFromTheSink(List<EventSink> eventSinks,
			String beanName) {
		for (EventSink sink : eventSinks) {
			if (beanName.equalsIgnoreCase(sink.getBeanName())) {
				return true;
			}
		}
		return false;
	}

	public boolean isPauseIfAnyEventSinkPauses() {
		return m_pauseIfAnyEventSinkPauses;
	}

	public void onApplicationEvent(ApplicationEvent event) {
		if (event instanceof PauseEvent) {
			processPauseEvent((PauseEvent) event);
		} else if (event instanceof ResumeEvent) {
			processResumeEvent((ResumeEvent) event);
		} else {
			applyConfig(event);
			processApplicationEvent(event);
		}
	}

	private void applyConfig(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {
			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

			if (bcInfo.isChangedBean((EventSinkList) getEventSinks())) {
				setEventSinks(((Collection<EventSink>) ((bcInfo
						.getApplicationContext()).getBean(bcInfo.getBeanName()))));
			}

		}
	}

	public abstract void pause();

	protected abstract void processApplicationEvent(ApplicationEvent event);

	/**
	 * 
	 * @param pauseEvent
	 */
	public void processPauseEvent(PauseEvent pauseEvent) {
		Object eventSource = pauseEvent.getSource();
		NamedBean bean = (NamedBean) eventSource;
		Collection<EventSink> eventSinks = getEventSinks();
		if (eventSinks != null) {
			for (EventSink eventSink : eventSinks) {
				if (bean.getBeanName()
						.equalsIgnoreCase(eventSink.getBeanName())) {
					removeEventSink(eventSink);
					m_pausedEventSink.add(eventSink);
				}
			}
			if (isEventFromTheSink(getPausedEventSink(), bean.getBeanName())
					&& m_pauseIfAnyEventSinkPauses && !m_isSourcePaused.get()) {
				m_isSourcePaused.set(true);
				pause();
			} else if (isEventFromTheSink(getPausedEventSink(),
					bean.getBeanName())
					&& getEventSinks().size() == 0 && !m_isSourcePaused.get()) {
				m_isSourcePaused.set(true);
				pause();
			}
		}
	}

	/**
	 * 
	 * @param resumeEvent
	 */
	public void processResumeEvent(ResumeEvent resumeEvent) {
		Object eventSource = resumeEvent.getSource();
		NamedBean bean = (NamedBean) eventSource;
		List<EventSink> eventSinks = new ArrayList<EventSink>();
		eventSinks
				.addAll(Collections.unmodifiableCollection(m_pausedEventSink));
		if (eventSinks != null) {
			for (EventSink eventSink : eventSinks) {
				if (bean.getBeanName()
						.equalsIgnoreCase(eventSink.getBeanName())) {
					addEventSink(eventSink);
					m_pausedEventSink.remove(eventSink);
				}
			}
			if (isEventFromTheSink(eventSinks, bean.getBeanName())
					&& m_pauseIfAnyEventSinkPauses
					&& getPausedEventSink().size() == 0
					&& m_isSourcePaused.get()) {
				m_isSourcePaused.set(false);
				resume();
			} else if (isEventFromTheSink(eventSinks, bean.getBeanName())
					&& !m_pauseIfAnyEventSinkPauses
					&& getEventSinks().size() > 0 && m_isSourcePaused.get()) {
				m_isSourcePaused.set(false);
				resume();
			}
		}
	}

	@ManagedOperation
	@Override
	public void clearErrorList() {
		m_errors.clearErrorList();
	}

	@Override
	public String getErrors() {
		return m_errors.toString();
	}

	@Override
	public void registerError(Throwable t) {
		m_errors.registerError(t);
	}

	@Override
	public void registerError(Throwable t, JetstreamEvent evtCause) {
		m_errors.registerError(t, evtCause);
	}

	public abstract void resume();

	@Override
	public void setErrorListMax(int nSize) {
		m_errors.setErrorListMax(nSize);
	}

	/**
	 * @param pausedEventSink
	 *            the pausedEventSink to set
	 */
	public void setPausedEventSink(List<EventSink> pausedEventSink) {
		m_pausedEventSink = pausedEventSink;
	}

	public void setPauseIfAnyEventSinkPauses(boolean pauseIfAnyEventSinkPauses) {
		m_pauseIfAnyEventSinkPauses = pauseIfAnyEventSinkPauses;
	}

	@Override
	public String toString() {
		return getBeanName();

	}

}
