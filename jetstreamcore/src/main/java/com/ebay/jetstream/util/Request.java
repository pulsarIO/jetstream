/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;





/**
* An implementation of a request thread pattern 
*
* *
* @author shmurthy@ebay.com
* @version 1.0
*/ 

public abstract class Request implements Runnable {
 
	public static final int LOW_PRIORITY = 1;
	public static final int HIGH_PRIORITY = 0;
	
	private int m_priority = 0;
	private long m_sequenceid = 0;
	
	

	
	public abstract boolean execute();
	/**
	 * @return the priority
	 */
	public int getPriority() {
		return m_priority;
	}

	/**
	 * @param priority the priority to set
	 */
	public void setPriority(int priority) {
		m_priority = priority;
	}

	/**
	 * @return the sequenceid
	 */
	public long getSequenceid() {
		return m_sequenceid;
	}

	/**
	 * @param sequenceid the sequenceid to set
	 */
	public void setSequenceid(long sequenceid) {
		m_sequenceid = sequenceid;
	}
	
	
	public void run() {
		execute();
	}
		
}
