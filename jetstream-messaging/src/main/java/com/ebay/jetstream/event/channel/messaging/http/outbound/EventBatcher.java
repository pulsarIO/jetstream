/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging.http.outbound;


import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.http.netty.client.HttpClient;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY")
public class EventBatcher implements Runnable, XSerializable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.http.outbound");
	private static final int BATCH_SIZE = 5;
	private static final int MAX_RETRIES = 3;
	private final ConcurrentHashMap<URI, ConcurrentLinkedQueue<JetstreamEvent>> m_urlEventQueue = new ConcurrentHashMap<URI, ConcurrentLinkedQueue<JetstreamEvent>>();
	private int m_batchSize = BATCH_SIZE;
	private int m_flushTime = 20; // 20 secs
	private final HttpClient m_client; 
	private LongEWMACounter m_eventsSentPerSec;
	private LongCounter m_totalEventsPosted = new LongCounter();
	private LongCounter m_totalEventsDropped = new LongCounter();
	private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
	private long m_maxQueueSize = 3000;
	private AtomicLong m_highWaterMark = new AtomicLong(0);
	private AtomicLong m_lowWaterMark = new AtomicLong(0);
	

	public long getEventsSentPerSec() {
		return m_eventsSentPerSec.get();
	}

	public long getTotalEventsPosted() {
		return m_totalEventsPosted.get();
	}

	public long getTotalEventsDropped() {
		return m_totalEventsDropped.get();
	}

		
	
	public EventBatcher(HttpClient client) {
		m_client = client;
		m_eventsSentPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
		
		m_highWaterMark.set((long) (0.85 * m_maxQueueSize));
		m_lowWaterMark.set((long) (0.25 * m_maxQueueSize));
		
	}

	public void flushQueue() {
		List<JetstreamEvent> eventList = new ArrayList<JetstreamEvent>();

		Set<Entry<URI, ConcurrentLinkedQueue<JetstreamEvent>>>  urlqueues = m_urlEventQueue.entrySet();

		Iterator<Entry<URI, ConcurrentLinkedQueue<JetstreamEvent>>> itr = urlqueues.iterator();

//		boolean allQueuesEmpty = true;

		while(itr.hasNext()) {
			ConcurrentLinkedQueue<JetstreamEvent> queue = itr.next().getValue();
			
			if (queue.isEmpty()) continue;
			
			URI uri = itr.next().getKey();

			while(!queue.isEmpty()) {
				eventList.clear();

				for (int i = 0; i < Math.min(m_batchSize, queue.size()); i++) {
					eventList.add(queue.poll());
				}

				publishEvents(uri, eventList);
				m_eventsSentPerSec.add(m_batchSize);				

			}
		}
	}

	/**
	 * @return the batchSize
	 */
	public int getBatchSize() {
		return m_batchSize;
	}

	/**
	 * @return the client
	 */
	public HttpClient getClient() {
		return m_client;
	}

	/**
	 * @return the flushTime
	 */
	public int getFlushTime() {
		return m_flushTime;
	}



	private void processQueues() {

		List<JetstreamEvent> eventList = new ArrayList<JetstreamEvent>();

		while (!m_shutdown.get()) {

			try {

	            boolean allQueuesEmpty = processURLQueue(eventList);

				if (allQueuesEmpty) {
					synchronized (this) {
						try {

							this.wait(m_flushTime * 1000);

						  
						} catch (InterruptedException e) {
							
							LOGGER.debug( e.getLocalizedMessage());
						}
					}
					flusheURLQueue(eventList);
                    if (m_shutdown.get()) {
                        break;
                    }
				}

			} catch (Throwable t) {
				LOGGER.error(
						"Http Request Dispatch Exception "
								+ t.getLocalizedMessage());
			}
		}
	}

    private boolean processURLQueue(List<JetstreamEvent> eventList) {
        boolean allQueuesEmpty = true;
         
        Set<Entry<URI, ConcurrentLinkedQueue<JetstreamEvent>>> urlqueues = m_urlEventQueue
        		.entrySet();

        Iterator<Entry<URI, ConcurrentLinkedQueue<JetstreamEvent>>> itr = urlqueues
        		.iterator();

        while (itr.hasNext()) {

        	Entry<URI, ConcurrentLinkedQueue<JetstreamEvent>> entry = itr
        			.next();

        	ConcurrentLinkedQueue<JetstreamEvent> queue = entry
        			.getValue();

        	URI uri = entry.getKey();

        	eventList.clear();
        	
        	if (queue.size() >= m_batchSize) {

        		for (int i = 0; i < m_batchSize; i++) {
        			eventList.add(queue.poll());
        		}

        		publishEvents(uri, eventList);

      			allQueuesEmpty = false;
        	}

        }
        return allQueuesEmpty;
    }

    private void flusheURLQueue(List<JetstreamEvent> eventList) {
        Set<Entry<URI, ConcurrentLinkedQueue<JetstreamEvent>>> urlqueues = m_urlEventQueue
                .entrySet();

        Iterator<Entry<URI, ConcurrentLinkedQueue<JetstreamEvent>>> itr = urlqueues
                .iterator();

        while (itr.hasNext()) {

            Entry<URI, ConcurrentLinkedQueue<JetstreamEvent>> entry = itr
                    .next();

            ConcurrentLinkedQueue<JetstreamEvent> queue = entry
                    .getValue();

            URI uri = entry.getKey();

            eventList.clear();
            
            for (int i = 0; i < m_batchSize; i++) {
                JetstreamEvent message = queue.poll();
                if (message != null) {
                    eventList.add(message);
                } else { 
                    break;
                }
            }

            if (!eventList.isEmpty()) {
                publishEvents(uri, eventList);
            } 
        }
    }

	private void publishEvents(URI uri, List<JetstreamEvent> eventList) {
		int retryCount = MAX_RETRIES;

		
		while (--retryCount > 0) {
			try {
				m_client.post(uri, eventList, null, null);
				m_eventsSentPerSec.add(m_batchSize);	
				m_totalEventsPosted.addAndGet(m_batchSize);
				break;
			}
			catch (Exception e) {
				LOGGER.error(
						"Http Request Post Exception "
								+ e.getLocalizedMessage());
				m_totalEventsDropped.addAndGet(m_batchSize);
			}
		}
	
		
	}

	@Override
	public void run() {

		
			processQueues();

			flushQueue(); // we will flush whatever is remaining in queue
	}

	/**
	 * @param batchSize
	 *          the batchSize to set
	 */
	public void setBatchSize(int batchSize) {
		m_batchSize = batchSize;
	}

	/**
	 * @param flushTime
	 *          the flushTime to set
	 */
	public void setFlushTime(int flushTime) {
		m_flushTime = flushTime;
	}

	/**
	 * @param shutdown
	 *          the shutdown to set
	 */
	public void shutdown() {
		m_shutdown.set(true);

		synchronized (this) {
			this.notifyAll(); 
		}

	}

	// public void submit(URI uri, JetstreamEvent event) throws Exception {
	public void submit(URI uri, JetstreamEvent event) throws Exception {
		if (m_shutdown.get())
			throw new Exception("shutting down");

		if (uri == null)
			throw new Exception("uri is null");
		
		if (!m_urlEventQueue.containsKey(uri)) {
			m_urlEventQueue.put(uri,
					new ConcurrentLinkedQueue<JetstreamEvent>());
		}
		
		ConcurrentLinkedQueue<JetstreamEvent> queue = m_urlEventQueue.get(uri);

		if (queue != null) {
			
			if (queue.size() > m_maxQueueSize)
				throw new Exception("Queue full for " + uri.toString());
						
			boolean bqueueInserted = queue.add(event);
				
			if (!bqueueInserted) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug( "Failed to insert in to queue");
					
				}
				m_totalEventsDropped.increment();
				return;
			}
			
			if (queue.size() >= m_batchSize) {
				synchronized (this) {
					this.notifyAll();
					
				}
			}
		}
	}
		
}
