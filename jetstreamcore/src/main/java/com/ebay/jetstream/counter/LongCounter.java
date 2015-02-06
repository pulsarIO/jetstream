/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.counter;

import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.xmlser.Hidden;

/**
 * @author shmurthy
 * 
 */
public class LongCounter {

	AtomicLong m_count = new AtomicLong(0);

	public long addAndGetHandlingRollover(long value) {

		if ((m_count.get() + value) < 0) {
			synchronized (this) {
				if ((m_count.get() + value) < 0)
					m_count.set(0);
			}
		}

		return m_count.addAndGet(value);
		
	}
	
	public long addAndGet(long value) {
		return m_count.addAndGet(value);
	}

	public long decrement() {

		if (m_count.get() < 1) {
			return 0;
		}

		return m_count.decrementAndGet();

	}

	public long get() {
		return m_count.get();

	}

	@Hidden
	public long getAndReset() {
		return m_count.getAndSet(0);

	}

	public long incrementHandlingRollover() {
		return addAndGetHandlingRollover(1);
	}
	
	public long increment() {

		return m_count.addAndGet(1);

	}

	public void reset() {
		m_count.set(0);
	}

	/**
	 * @param l
	 */
	public void set(long val) {
		m_count.set(val);

	}
	
	public String toString() {
		return m_count.toString();
	}
}
