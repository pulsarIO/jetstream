/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/

package com.ebay.jetstream.event.processor.loadbalance;

/**
*
* This component provides an implementation of a Load Balancer which distributes the events in
* round robin fashion to its sinks. This component can be used to distribute load amongst its sinks
* to take advantage of cores. Typically EsperProcessor is single threaded. One can deploy multiple
* EsperProcessor instances and distribute load amongst.
* 
* @author shmurthy@ebay.com
*
*/

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.xmlser.XSerializable;
import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.management.Management;

@ManagedResource(objectName = "Event/Processor", description = "Load Balancer for balancing event traffic amongst sinks")
public class RoundRobinLoadBalancer  extends AbstractEventProcessor implements XSerializable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.processor.loadbalance");

	private int m_numSinks = -1;
	private int m_nextSinkPos = 0;
	private EventSink[] m_sinks = new EventSink[20];
	
	
	@Override
	public void sendEvent(JetstreamEvent event) throws EventException {
		
		incrementEventRecievedCounter();
		
		if (m_numSinks != getEventSinks().size()) {
			
			Collection<EventSink> sinks = getEventSinks();
			sinks.toArray(m_sinks);
			m_numSinks = sinks.size();
			m_nextSinkPos = 0;
			
		}
		
		if (m_numSinks > 0) {
			
			if (m_nextSinkPos >= m_numSinks)
				m_nextSinkPos = 0;
			
			m_sinks[m_nextSinkPos].sendEvent(event);
			m_nextSinkPos+=1;
			incrementEventSentCounter();
		}
		
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		
		Management.removeBeanOrFolder(getBeanName(), this);
		Management.addBean(getBeanName(), this);
				
		Collection<EventSink> sinks = getEventSinks();
		sinks.toArray(m_sinks);
		m_numSinks = sinks.size();
		
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}

	@Override
	public void shutDown() {
		
		
	}

	@Override
	public void pause() {
		
		if (isPaused()) {
			LOGGER.warn( getBeanName() + " could not be resumed. It is already in paused state");
			return;
		}
		
		changeState(ProcessorOperationState.PAUSE);
	}

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
				
	}

	@Override
	public void resume() {
		
		if (!isPaused()) {
		      LOGGER.warn( getBeanName() + " could not be resumed. It is already in resumed state");
	   	      return;
		}
		
		changeState(ProcessorOperationState.RESUME);
		
	}

}
