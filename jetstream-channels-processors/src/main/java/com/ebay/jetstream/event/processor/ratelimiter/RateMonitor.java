/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.ratelimiter;

import java.util.concurrent.atomic.AtomicLong;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.xmlser.XSerializable;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_FIELD")

public class RateMonitor implements XSerializable {

	private AtomicLong m_rateThreshold = new AtomicLong(45000); // 45k is default
	private LongCounter m_counter = new LongCounter();
	private AtomicLong m_peakRate = new AtomicLong();
	AtomicLong m_prevTime = new AtomicLong(0);
	
	public AtomicLong getPeakRate() {
		return m_peakRate;
	}

	public void setPeakRate(AtomicLong m_peakRate) {
		this.m_peakRate = m_peakRate;
	}

	public long getRateThreshold() {
		return m_rateThreshold.get();
	}

	public void setRateThreshold(long m_rateThreshold) {
		this.m_rateThreshold.set(m_rateThreshold);
	}

	public RateMonitor() {}
	
	public boolean incrementAndCheckRate() {
		m_counter.increment();
		long counterVal = m_counter.get();
		m_peakRate.set(Math.max(counterVal, m_peakRate.get()));
					
		// now we will do a time check to account for clock skew from timer.
		// if Timer has not kicked in we will reset the count
		
		if (m_prevTime.get() == 0)
			m_prevTime.set(System.currentTimeMillis());
		else {
			long curTime = System.currentTimeMillis();
			if ((curTime - m_prevTime.get()) > 1000) {
				m_prevTime.set(curTime);				
				m_counter.reset();
			}
		}
		if (counterVal > m_rateThreshold.get())
			return true;
		else
			return false;
	}
	
	
}
