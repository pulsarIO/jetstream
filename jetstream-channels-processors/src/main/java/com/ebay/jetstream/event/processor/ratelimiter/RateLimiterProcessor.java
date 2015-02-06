/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.ratelimiter;


/**
*
* This component uses memory monitor to determine used memory and when
* used memory crosses a high water mark it pauses the pipeline and when
* used memory falls below a low water mark it will resume the pipeline.
*
* @author shmurthy@ebay.com
*
*/


import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.XSerializable;



@ManagedResource(objectName = "Event/Processor", description = "Rate Limiter based on JVM memory usage")
public class RateLimiterProcessor extends AbstractEventProcessor implements XSerializable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.processor.ratelimiter");
	private RateLimiterProcessorConfig m_config = new RateLimiterProcessorConfig();
	private AtomicBoolean m_rateLimit = new AtomicBoolean();
	private RateMonitor m_rateMonitor;
	private ConcurrentHashMap<Object, Long> m_topNRegistry = new ConcurrentHashMap<Object, Long>();
	private Tracker m_tracker;
	private LongCounter m_highFrequencyItemDropCounter = new LongCounter();
	
	
	public long getHighFrequenyItemDropCounter() {
		return m_highFrequencyItemDropCounter.get();
	}

	public Tracker getTracker() {
		return m_tracker;
	}

	public Map<Object, Long> getTopNRegistry() {
		return Collections.unmodifiableMap(m_topNRegistry);
	}
	
	public RateMonitor getRateMonitor() {
		return m_rateMonitor;
	}

	public void setRateMonitor(RateMonitor m_rateMonitor) {
		this.m_rateMonitor = m_rateMonitor;
	}

	
	public RateLimiterProcessor() {
		
	}
	
		
	public RateLimiterProcessorConfig getConfig() {
		return m_config;
	}


	public void setConfig(RateLimiterProcessorConfig config) {
		m_config = config;
		m_rateLimit.set(config.isRateLimit());
		
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see orm_memoryMonitorg.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		
		 Management.removeBeanOrFolder(getBeanName(), this);
		    Management.addBean(getBeanName(), this);
		
		
		if (m_rateMonitor == null) {
			m_rateMonitor = new RateMonitor();
			
		}
				
		if (m_tracker == null) {
			m_tracker = new Tracker();
			m_tracker.start();
		}
		
		m_rateMonitor.setRateThreshold(getConfig().getRateThreshold());
	
			
	}


	
	@ManagedOperation
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
				RateLimiterProcessorConfig newConfig = (RateLimiterProcessorConfig) bcInfo.getChangedBean(); 
				setConfig(newConfig);
				// Need to update the config to RateLimiter 
				m_rateMonitor.setRateThreshold(newConfig.getRateThreshold());
			}
		}

	}

	
	@Override
	public void sendEvent(JetstreamEvent event) throws EventException {
		incrementEventRecievedCounter();

		if (m_rateLimit.get()) {
			Object item = event.get(m_config.getKey());

			// first we will attempt to drop events which are exceeding the item
			// count threshold

			if ((item != null) && (m_config.getItemCountThreshold() > 0)) {

				// this should help reduce pressure on downstream

				m_tracker.offer(item);

				if (m_tracker.isItemBlocked(item)) {
					
					if (getAdviceListener() != null) {

						JetstreamEvent newEvent = new JetstreamEvent(event);

						newEvent.put(
								JetstreamReservedKeys.AbusiveEntity.toString(),
								m_tracker.getTopNRegistry());

						getAdviceListener().retry(newEvent,
								RetryEventCode.RATEPROCESSOR_OVERFLOW,
								"rate exceeded");
						
						
					}
					
					m_highFrequencyItemDropCounter.increment();
					
					incrementEventDroppedCounter();

					return;

				}
			}

			// finally if we have crossed rate threshold we will have to drop
			// randomly

			if (m_rateMonitor.incrementAndCheckRate()) {
				// if we are here we have exceeded rate

				// Now let's check if this item being tracked is flagged as the
				// one to block
				// if true we will drop the event

				if (getAdviceListener() != null)
					getAdviceListener().retry(event,
							RetryEventCode.RATEPROCESSOR_OVERFLOW,
							"rate exceeded");
				else
					incrementEventDroppedCounter();

				return;

			}
		}

		try {
			fireSendEvent(event);
			incrementEventSentCounter();
		} catch (Throwable t) {
			incrementEventDroppedCounter();
			LOGGER.warn( t.getLocalizedMessage());
		}
	}

	@Override
	@ManagedOperation
	public void resume() {
		
		if (!isPaused()) {
		      LOGGER.warn( getBeanName() + " could not be resumed. It is already in resumed state");
	   	      return;
		}
		
		changeState(ProcessorOperationState.RESUME);
		
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}


	@Override
	public void shutDown() {
			
		if (m_tracker != null)
			m_tracker.shutDown();
	}
	
	
}
