/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.ratelimiter;

/**
*
* *
* @author shmurthy@ebay.com
*
*/


import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;



public class RateLimiterProcessorConfig extends AbstractNamedBean implements XSerializable {

	private long m_rateThreshold = 18000;
	private boolean m_rateLimit = false;
	private String m_key;
	private AtomicLong m_itemCountThreshold = new AtomicLong(70);
	
			

	public String getKey() {
		return m_key;
	}

	public void setKey(String key) {
		m_key = key;
	}
	
	public long getItemCountThreshold() {
		return m_itemCountThreshold.get();
	}

	public void setItemCountThreshold(long itemCountThreshold) {
		m_itemCountThreshold.set(itemCountThreshold);
	}
	
	public boolean isRateLimit() {
		return m_rateLimit;
	}

	public void setRateLimit(boolean rateLimit) {
		this.m_rateLimit = rateLimit;
	}

	public RateLimiterProcessorConfig() {
		
	}
	
	public long getRateThreshold() {
		return m_rateThreshold;
	}

	public void setRateThreshold(long rateThreshold) {
		this.m_rateThreshold = rateThreshold;
	}

	
}
