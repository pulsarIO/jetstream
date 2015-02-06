/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.batcher;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author shmurthy@ebay.com - This is a generic batcher that batches data items of the specified type and flushes the batch of data items. The flush is triggered when
 * 							   the batch queue contains items equal to the specified threshold or the flush timer fires. The threshold and flush timer interval are both
 * 							   configurable.
 */

public abstract class AutoFlushBatcher<T> implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private AtomicInteger m_flushInterval = new AtomicInteger(100);
	private AtomicInteger m_maxFlushSz = new AtomicInteger(40);
	private AutoFlushWriterQueue m_queue;
	private Thread m_batchAssembler;
	private AtomicBoolean m_shutdown = new AtomicBoolean(false);
	private int m_maxQueueSz = 100000;
	private int m_waitBetweenFlush = 30; // in millis - default = 30 millis

	public int getWaitBetweenFlush() {
		return m_waitBetweenFlush;
	}

	public void setWaitBetweenFlush(int waitBetweenFlush) {
		m_waitBetweenFlush = waitBetweenFlush;
	}

	public int getMaxQueueSz() {
		return m_maxQueueSz;
	}

	public void setMaxQueueSz(int maxQueueSz) {
		m_maxQueueSz = maxQueueSz;
	}

	public int getFlushInterval() {
		return m_flushInterval.get();
	}

	public void setFlushInterval(int flushInterval) {
		m_flushInterval.set(flushInterval);
	}

	public void start() {
		m_batchAssembler = new Thread(this, "BatchAssemblerThread");
		m_batchAssembler.start();
	}

	/**
	 * @return
	 */
	public int getMaxFlushSz() {
		return m_maxFlushSz.get();
	}

	/**
	 * @param maxFlushSz
	 */
	public void setMaxFlushSz(int maxFlushSz) {
		this.m_maxFlushSz.set(maxFlushSz);
	}

	/**
	 * 
	 */
	public AutoFlushBatcher() {
		m_queue = new AutoFlushWriterQueue<T>();
		m_queue.setMaxFlushSz(m_maxFlushSz.get());
		start();

	}

	/**
	 * @param autoFlushSz
	 * @param flushInterval
	 *            in millis
	 */
	public AutoFlushBatcher(int autoFlushSz, int flushInterval) {

		m_maxFlushSz.set(autoFlushSz);
		m_flushInterval.set(flushInterval);
		m_queue = new AutoFlushWriterQueue<T>();
		m_queue.setMaxFlushSz(m_maxFlushSz.get());
		start();
	}

	/**
	 * @param e
	 */
	public void write(T e) throws Exception {
		if (m_shutdown.get())
			throw new Exception("Batcher has been shutdown");

		m_queue.add(e);
	}

	/**
	 * @param items
	 *            - derived class to implement this method
	 */

	public abstract void flush(List<T> items);

	/**
	 * 
	 */
	public void shutdown() {

		m_shutdown.set(true);

		LOGGER.info( "Shutting down AutoFlushBufferWriter");

		while (m_queue.size() > 0) {

			List<T> events = m_queue.get(getMaxFlushSz());

			if (events != null)
				flush(events);

		}

		
	}

	@Override
	public void run() {

		while (!m_shutdown.get()) {

			long elapsedTime = System.currentTimeMillis()
					- m_queue.getLastFlushTime();

			if (m_queue.isTimeToFlush()
					|| (elapsedTime > m_flushInterval.get())) {

				m_queue.setLastFlushTime(System.currentTimeMillis());

				List<T> events = m_queue.get(getMaxFlushSz());

				if (events == null)
					continue;

				flush(events);

			} else {
				try {
					Thread.sleep(getWaitBetweenFlush()); // wait 30 millis

				} catch (InterruptedException e) {

				}
			}
		}
	}

}
