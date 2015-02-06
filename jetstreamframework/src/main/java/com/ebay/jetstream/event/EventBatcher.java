/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY")

public class EventBatcher implements Runnable {

		
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event");
	private static final int BATCH_SIZE = 5;
	private static final int MAX_RETRIES = 3;
	private final ConcurrentLinkedQueue<JetstreamEvent> m_queue = new ConcurrentLinkedQueue<JetstreamEvent>();
	private int m_batchSize = BATCH_SIZE;
	private int m_flushTime = 20; // 20 secs
	private int m_maxQueueSz = 5000;

	public EventBatcher() {}
	
	public int getMaxQueueSz() {
		return m_maxQueueSz;
	}

	public void setMaxQueueSz(int m_maxQueueSz) {
		this.m_maxQueueSz = m_maxQueueSz;
	}

	private final AtomicBoolean m_shutdown = new AtomicBoolean(false);
	private BatchEventSink m_batchEventSink;
	private Thread m_queueProcessor;
	
	public void start() {
		m_queueProcessor = new Thread(this, "EventBatcherThread");
		m_queueProcessor.start();
	}
	
	public BatchEventSink getBatchEventSink() {
		return m_batchEventSink;
	}

	public void setBatchEventSink(BatchEventSink m_batchEventSink) {
		this.m_batchEventSink = m_batchEventSink;
	}

	public EventBatcher(BatchEventSink sink) {
		m_batchEventSink = sink;
	}

	public void flushQueue() {
		List<JetstreamEvent> eventList = new ArrayList<JetstreamEvent>();


		while(!m_queue.isEmpty()) {
			eventList.clear();

			for (int i = 0; i < Math.min(m_batchSize, m_queue.size()); i++) {
				eventList.add(m_queue.poll());
			}

			writeEvents(eventList);

		}
	}

	/**
	 * @return the batchSize
	 */
	public int getBatchSize() {
		return m_batchSize;
	}



	/**
	 * @return the flushTime
	 */
	public int getFlushTime() {
		return m_flushTime;
	}



	private void processQueue() {

		List<JetstreamEvent> eventList = new ArrayList<JetstreamEvent>();

		while(!m_shutdown.get()) {

			try {

				eventList.clear();

				if (m_queue.size() != 0) {

					for (int i = 0; i < m_batchSize; i++) {
						eventList.add(m_queue.poll());
					}

					writeEvents(eventList);

				}
				synchronized (this) {
					try {
						this.wait(m_flushTime * 1000);
					}
					catch (InterruptedException e) {
						LOGGER.debug( "EventBatcher:processQueue wait interrupted");
					}
					if (m_shutdown.get())
						break;
				}

			} catch (Throwable t) {
				LOGGER.error( "EventBatcher:processQueue Exception " + t.getLocalizedMessage());
			}
		}
	}


	private void writeEvents(List<JetstreamEvent> eventList) {
		int retryCount = MAX_RETRIES;

		while (--retryCount > 0) {

			m_batchEventSink.sendEvents(eventList, null);
			
			break;

		}
	}

	@Override
	public void run() {
		
		try {
			processQueue();

			flushQueue(); // we will flush whatever is remaining in queue
		} catch (Throwable t) {
			LOGGER.error( "EventBatcher:run - shutting down");
		}
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

	public void submit(JetstreamEvent event) throws Exception {
		if (m_shutdown.get())
			throw new Exception("shutting down");
		

		if (m_queue.size() >= m_maxQueueSz)
			throw new Exception("Queue full");

		m_queue.add(event);

		if (m_queue.size() >= m_batchSize)
			synchronized (this) {
				this.notifyAll();  
			}
	}
}
