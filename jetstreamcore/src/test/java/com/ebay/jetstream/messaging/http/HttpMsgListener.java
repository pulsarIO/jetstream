/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.http;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpMsgListener {
	private AtomicInteger m_count = new AtomicInteger(0);
	
	private HashMap<String, String> m_map;
	private HttpMsgTest m_hmt;
	private String m_encoder=null;
	
	public int getCount() {
		return m_count.get();
	}

	public void setCount(int count) {
		m_count.set(count);
	}

	public HttpMsgListener(HashMap<String, String> m, HttpMsgTest hmt) {
		m_map = m;
		m_hmt = hmt;
	}

	public void onMessage(Object o) {
		
		if (o instanceof java.util.HashMap) {
			//m_hmt.setTestPassed(new AtomicBoolean(false));
			if (o.equals(m_map)) {
				
				System.out.println("expected message received:" + m_encoder); //KEEPME
				m_hmt.setTestPassed(new AtomicBoolean(true));
			}
			else {
				System.out.println("expected message not received:" + m_encoder);
				m_hmt.setTestPassed(new AtomicBoolean(false));
			}
			
			m_count.addAndGet(1);
		
		}

	}

	public String getEncoder() {
		return m_encoder;
	}

	public void setEncoder(String encoder) {
		this.m_encoder = encoder;
	}

}
