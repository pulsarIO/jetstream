/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.config;

import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler;
import com.ebay.jetstream.messaging.transport.netty.eventscheduler.WeightedRoundRobinScheduler;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * 
 * Netty specific context config
 */

public class NettyContextConfig extends ContextConfig implements XSerializable {

	private Scheduler m_scheduler = new WeightedRoundRobinScheduler();
	private int m_weight = 100;
	private boolean m_backupAffinityNode = false;
	private int m_port = -1;
	

	public int getPort() {
		return m_port;
	}

	public void setPort(int port) {
		this.m_port = port;
	}

	/**
	 * @return
	 */
	public Scheduler getScheduler() {
		return m_scheduler;
	}

	/**
	 * @param m_scheduler
	 */
	public void setScheduler(Scheduler m_scheduler) {
		this.m_scheduler = m_scheduler;
	}

	/**
	 * @return
	 */
	public int getWeight() {
		return m_weight;
	}

	/**
	 * @param m_weight
	 */
	public void setWeight(int m_weight) {
		this.m_weight = m_weight;
	}

	/**
	 * @return
	 */
	public boolean isBackupAffinityNode() {
		return m_backupAffinityNode;
	}

	/**
	 * @param m_backupAffinityNode
	 */
	public void setBackupAffinityNode(boolean m_backupAffinityNode) {
		this.m_backupAffinityNode = m_backupAffinityNode;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj == this)
			return true;

		if (obj == null)
			return false;

		if (!(obj instanceof NettyContextConfig))
			return false;

		NettyContextConfig cc = (NettyContextConfig) obj;

		if (m_backupAffinityNode != cc.m_backupAffinityNode)
			return false;

		if (!m_scheduler.getClass().toString().equals(cc.getScheduler().getClass().toString()))
			return false;

		if (m_weight != cc.getWeight())
			return false;
		
		return super.equals(cc);
		
	}
	
	public int hashCode() {
		
		return Integer.valueOf(m_weight).hashCode() + m_scheduler.hashCode();
		
	}
}
