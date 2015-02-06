/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author shmurthy
 *
 * 
 */
public class DispatchQueueStats {

	private AtomicInteger m_lowPriorityQueueDepth = new AtomicInteger(0);
	private AtomicInteger m_highPriorityQueueDepth = new AtomicInteger(0);
	private AtomicInteger m_maxQueueDepth = new AtomicInteger(0);
	/**
	 * @return the maxQueueDepth
	 */
	public int getMaxQueueDepth() {
		return m_maxQueueDepth.get();
	}
	/**
	 * @param maxQueueDepth the maxQueueDepth to set
	 */
	public void setMaxQueueDepth(int maxQueueDepth) {
		m_maxQueueDepth.set(maxQueueDepth);
	}
	/**
	 * @return the highPriorityQueueDepth
	 */
	public int getHighPriorityQueueDepth() {
		return m_highPriorityQueueDepth.get();
	}
	/**
	 * @param highPriorityQueueDepth the highPriorityQueueDepth to set
	 */
	public void setHighPriorityQueueDepth(int highPriorityQueueDepth) {
		m_highPriorityQueueDepth.set(highPriorityQueueDepth);
	}
	/**
	 * @return the lowPriorityQueueDepth
	 */
	public int getLowPriorityQueueDepth() {
		return m_lowPriorityQueueDepth.get();
	}
	/**
	 * @param lowPriorityQueueDepth the lowPriorityQueueDepth to set
	 */
	public void setLowPriorityQueueDepth(int lowPriorityQueueDepth) {
		m_lowPriorityQueueDepth.set(lowPriorityQueueDepth);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
      
      String statsStr = "current low priority Queue Depth = ";
      
      statsStr += getLowPriorityQueueDepth();
      statsStr += "\n";
      statsStr += "current high priority Queue Depth = ";
      statsStr += getHighPriorityQueueDepth();
      statsStr += "\n";
      statsStr += "current Max Queue Depth = ";
      statsStr += getMaxQueueDepth();
      
      return statsStr;
      
    }
    
}
