/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.stats;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy
 * 
 * 
 */

@ManagedResource
public class MessageServiceStats implements XSerializable {

	private long m_totalMsgsSent;
	private long m_msgsSentPerSec;
	private long m_totalMsgsRcvd;
	private long m_msgsRcvdPerSec;
	private long m_totalMsgsLost;
	private long m_totalMsgsLostByNoContext;
	private int m_highPriorityQueueDepth;
	private int m_lowPriorityQueueDepth;
	private boolean m_paused;

	/**
	 * 
	 * @return status on Upstream Dispatcher Queue
	 */
	public boolean getPaused() {
		return m_paused;
	}

	public void setPaused(
			boolean paused) {
		m_paused = paused;
	}

	/**
	 * @return the msgsRcvdPerSec
	 */
	public long getMsgsRcvdPerSec() {
		return m_msgsRcvdPerSec;
	}

	/**
	 * @param msgsRcvdPerSec
	 *            the msgsRcvdPerSec to set
	 */
	public void setMsgsRcvdPerSec(long msgsRcvdPerSec) {
		m_msgsRcvdPerSec = msgsRcvdPerSec;
	}

	/**
	 * @return the msgsSentPerSec
	 */
	public long getMsgsSentPerSec() {
		return m_msgsSentPerSec;
	}

	/**
	 * @param msgsSentPerSec
	 *            the msgsSentPerSec to set
	 */
	public void setMsgsSentPerSec(long msgsSentPerSec) {
		m_msgsSentPerSec = msgsSentPerSec;
	}

	/**
	 * @return the totalMsgsRcvd
	 */
	public long getTotalMsgsRcvd() {
		return m_totalMsgsRcvd;
	}

	/**
	 * @param totalMsgsRcvd
	 *            the totalMsgsRcvd to set
	 */
	public void setTotalMsgsRcvd(long totalMsgsRcvd) {
		m_totalMsgsRcvd = totalMsgsRcvd;
	}

	/**
	 * @return the totalMsgsSent
	 */
	public long getTotalMsgsSent() {
		return m_totalMsgsSent;
	}

	/**
	 * @param totalMsgsSent
	 *            the totalMsgsSent to set
	 */
	public void setTotalMsgsSent(long totalMsgsSent) {
		m_totalMsgsSent = totalMsgsSent;
	}

	/**
	 * @return the highPriorityQueueDepth
	 */
	public int getHighPriorityQueueDepth() {
		return m_highPriorityQueueDepth;
	}

	/**
	 * @param highPriorityQueueDepth
	 *            the highPriorityQueueDepth to set
	 */
	public void setHighPriorityQueueDepth(int highPriorityQueueDepth) {
		m_highPriorityQueueDepth = highPriorityQueueDepth;
	}

	/**
	 * @return the lowPriorityQueueDepth
	 */
	public int getLowPriorityQueueDepth() {
		return m_lowPriorityQueueDepth;
	}

	/**
	 * @param lowPriorityQueueDepth
	 *            the lowPriorityQueueDepth to set
	 */
	public void setLowPriorityQueueDepth(int lowPriorityQueueDepth) {
		m_lowPriorityQueueDepth = lowPriorityQueueDepth;
	}

	/**
	 * @return the totalMsgsLost
	 */
	public long getTotalMsgsLost() {
		return m_totalMsgsLost;
	}

	/**
	 * @param totalMsgsLost
	 *            the totalMsgsLost to set
	 */
	public void setTotalMsgsLost(long totalMsgsLost) {
		m_totalMsgsLost = totalMsgsLost;
	}

	public long getTotalMsgsLostByNoContext() {
		return m_totalMsgsLostByNoContext;
	}

	public void setTotalMsgsLostByNoContext(long totalMsgsLostByNoContext) {
		this.m_totalMsgsLostByNoContext = totalMsgsLostByNoContext;
	}
}
