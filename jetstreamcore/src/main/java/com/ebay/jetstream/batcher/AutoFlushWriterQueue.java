/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.batcher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.util.disruptor.SingleConsumerDisruptorQueue;




/**
 * @author shmurthy@ebay.com - queue that holds items to be batched by the AutoFlushBatcher
 */

public class AutoFlushWriterQueue<T> {

	private SingleConsumerDisruptorQueue m_queue;
	
	// LinkedBlockingQueue<T> m_queue = new LinkedBlockingQueue<T>();
	private int m_maxQueueSz = 100000;
	private AtomicInteger m_maxFlushSz = new AtomicInteger(40);
	private AtomicLong m_lastFlushTime = new AtomicLong(0);
	
	public int getMaxFlushSz() {
		return m_maxFlushSz.get();
	}

	public void setMaxFlushSz(int m_maxFlushSz) {
		this.m_maxFlushSz.set(m_maxFlushSz);
	}

	public int getMaxQueueSz() {
		return m_maxQueueSz;
	}

	public void setMaxQueueSz(int maxQueueSz) {
		m_maxQueueSz = maxQueueSz;
	}

	public long getLastFlushTime() {
		return m_lastFlushTime.get();
	}

	public void setLastFlushTime(long lastFlushTime) {
		this.m_lastFlushTime.set(lastFlushTime);
	}

	public AutoFlushWriterQueue() {
		this(100, 10000);
	}

	public AutoFlushWriterQueue(int maxFlushSz, int maxQueueSz) {
		m_maxFlushSz.set(maxFlushSz);
		m_maxQueueSz = maxQueueSz;
		m_queue = new SingleConsumerDisruptorQueue(maxQueueSz);
	}

	/**
	 * return true if time to flush else return false
	 * 
	 * @param e
	 * @return
	 * @throws Exception
	 */
	public void add(T e) throws Exception {

		if (m_queue.size() < m_maxQueueSz) {
			m_queue.offer(e);
		}
		else
			throw new Exception("Queue Full");

	}

	/**
	 * @return true if bufsize is >= max flush buf size else false
	 */
	public boolean isTimeToFlush() {
		if (m_queue.size() >= m_maxFlushSz.get()) {
			return true;
		} else
			return false;
	}

	public int size() {
		return m_queue.size();
	}

	/**
	 * @return
	 */

	public List<T> get(int count) {

		if (m_queue.isEmpty())
			return null;

		ArrayList l = new ArrayList(count);
	    
		m_queue.drainTo(l, count);
		
		return l;
		
		
		
	}

	/**
	 * 
	 */
	public void clear() {
		m_queue.clear();

	}

}
