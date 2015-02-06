/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/

package com.ebay.jetstream.event.processor.loadbalance;

/**
*
* This component provides an implementation of a Load Balancer which distributes the events to its sinks in
* a partitioned scheme. A list of keys from the event can be used to create a partition key. A hash of
* the composite key mod num sinks determines the sink to schedule the event to. 
* 
* @author shmurthy@ebay.com
*
*/

import java.util.Collection;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.XSerializable;

@ManagedResource(objectName = "Event/Processor", description = "Load Balancer for balancing event traffic amongst sinks")
public class PartitionedLoadBalancer  extends AbstractEventProcessor implements XSerializable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.processor.loadbalance");

	private int m_numSinks = -1;
	private EventSink[] m_sinks = new EventSink[20];
	private PartionedLoadBalancerConfig m_config;
	
	
	public PartionedLoadBalancerConfig getConfig() {
		return m_config;
	}

	public void setConfig(PartionedLoadBalancerConfig config) {
		this.m_config = config;
	}

				
	@Override
	public void sendEvent(JetstreamEvent event) throws EventException {
		
		incrementEventRecievedCounter();
		
		if (getConfig().getPartitionKeys().isEmpty()) {
			incrementEventDroppedCounter();
			return;
		}
		
		if (m_numSinks != getEventSinks().size()) {
			
			Collection<EventSink> sinks = getEventSinks();
			sinks.toArray(m_sinks);
			m_numSinks = sinks.size();
					
		}
		
		int pos = 0;
		
		if (m_numSinks > 0) {
			
			try {
				long hash = genHash(event);
				if (hash < 0)
					hash *= -1;
				
				pos = (int) (hash % m_numSinks);
			} catch (Exception e) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Failed to generate Hash - " + e.getLocalizedMessage());
				incrementEventDroppedCounter();
				return;
			}
			
			m_sinks[pos].sendEvent(event);
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
		if (event instanceof ContextBeanChangedEvent) {
			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;
			// Calculate changes
			if (bcInfo.isChangedBean(getConfig())) {
				PartionedLoadBalancerConfig newConfig = (PartionedLoadBalancerConfig) bcInfo.getChangedBean(); 
				setConfig(newConfig);
				
			}
		}
	}

	@Override
	public void resume() {
		
		if (!isPaused()) {
		      LOGGER.warn( getBeanName() + " could not be resumed. It is already in resumed state");
	   	      return;
		}
		
		changeState(ProcessorOperationState.RESUME);
		
	}
	
	private long genHash(JetstreamEvent event) throws Exception {
		
		List<String> partitionfields = getConfig().getPartitionFields(event.getEventType());
		
		if (partitionfields == null)
			throw new RuntimeException("could not find fields for event type : " + event.getEventType());
		
		long hash = 0;
		
		for (String key : partitionfields) {
			hash += (31 * event.get(key).hashCode());
		}
						
		return hash;
			
	}
}