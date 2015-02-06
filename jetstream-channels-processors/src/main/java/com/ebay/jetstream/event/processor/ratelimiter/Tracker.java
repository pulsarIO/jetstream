/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.ratelimiter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.ebay.jetstream.util.disruptor.SingleConsumerDisruptorQueue;
import com.ebay.jetstream.xmlser.XSerializable;

/**
*
* This class is a helper for RateLimiterProcessor. It's purpose is to track the frequency distribution of the items which are fields in an item.
* The items which constitute the top 10 in the distribition will be penalized by picking them as candidates to be blocked
* 
* @author shmurthy@ebay.com
*
*/

public class Tracker implements Runnable, XSerializable {

	private StreamSummary<Object> m_topElementCounter;
	private ConcurrentHashMap<Object, Long> m_topNRegistry = new ConcurrentHashMap<Object, Long>();
	private AtomicBoolean m_shutDown = new AtomicBoolean(false);
	private AtomicLong m_prevTime = new AtomicLong(0);
	private SingleConsumerDisruptorQueue<Object> m_queue = new SingleConsumerDisruptorQueue<Object>(
			10000);
	private RateLimiterProcessorConfig m_config = new RateLimiterProcessorConfig();
	private Thread m_trackerThread;
	

	public boolean getShutDown() {
		return m_shutDown.get();
	}
	
	public boolean isShutDown() {
		return m_shutDown.get();
	}

	public void shutDown() {
		m_shutDown.set(true);
	}
	
	public Map<Object, Long> getTopNRegistry() {
		return Collections.unmodifiableMap(m_topNRegistry);
	}
	
	public void start() {
		
		m_trackerThread = new Thread(this);
		m_trackerThread.setName("RateLimiterProcessorTrackerThread");
		m_trackerThread.start();
	}

	private void refreshTopNRegistry() {

		if (m_prevTime.get() == 0)
			m_prevTime.set(System.currentTimeMillis());
		else {
			long curTime = System.currentTimeMillis();
			
			if ((curTime - m_prevTime.get()) > 5000) {

				m_prevTime.set(curTime);

				// now it is time to refresh our top N registry

				m_topNRegistry.clear();

				List<Counter<Object>> topCounters = m_topElementCounter
						.topK(10); // we will get the top 10

				for (Counter<Object> counter : topCounters) {
					m_topNRegistry.put(counter.getItem(), counter.getCount());
				}

			}
		}

	}

	public void offer(Object itemToTrack) {
		m_queue.offer(itemToTrack);
	}

	public boolean isItemBlocked(Object item) {

		Long count = m_topNRegistry.get(item);

		if ((count != null)
				&& (count.longValue() > m_config.getItemCountThreshold())) {
			return true;
		}
		else
			return false;
	}

	@Override
	public void run() {

		m_topElementCounter = new StreamSummary<Object>(1000);
		
		long prevTime = System.currentTimeMillis();
		
		while (!isShutDown()) {

			long currTime = System.currentTimeMillis();
			
			if ((currTime - prevTime) > 30000) {
				
				// every 15 secs we will create a new summary
		
				prevTime = currTime;
				m_topElementCounter = new StreamSummary<Object>(1000);
				
			}
		
			
			try {
				m_topElementCounter.offer(m_queue.take());
			} catch (Throwable e) {
				// swallow the exception
			}
			
			refreshTopNRegistry();

		}

	}

}
