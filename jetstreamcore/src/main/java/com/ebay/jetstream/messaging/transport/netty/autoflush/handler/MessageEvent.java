/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.autoflush.handler;

import io.netty.util.concurrent.Promise;

/**
 * @author shmurthy@ebay.com - Message Event -- holder of event
 */

public class MessageEvent implements Comparable<MessageEvent> {
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_msg == null) ? 0 : m_msg.hashCode());
		result = prime * result
				+ ((m_promise == null) ? 0 : m_promise.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageEvent other = (MessageEvent) obj;
		if (m_msg == null) {
			if (other.m_msg != null)
				return false;
		} else if (!m_msg.equals(other.m_msg))
			return false;
		if (m_promise == null) {
			if (other.m_promise != null)
				return false;
		} else if (!m_promise.equals(other.m_promise))
			return false;
		return true;
	}
	private Object m_msg;
	public Object getMsg() {
		return m_msg;
	}
	public void setMsg(Object msg) {
		this.m_msg = msg;
	}
	private Promise m_promise;
	
	public Promise getPromise() {
		return m_promise;
	}
	public void setPromise(Promise promise) {
		this.m_promise = promise;
	}
	
	public MessageEvent(Object msg, Promise promise) {
		m_msg = msg;
		m_promise = promise;
	
	}
	
	
	@Override
	public int compareTo(MessageEvent o) {
		
	    if ((o.m_msg == this.m_msg) && (o.m_promise != this.m_promise))
	    		return 0;
	    
	    if (o.m_msg != this.m_msg)
	    	    return -1;
	    
	    return 1;
	}

}
