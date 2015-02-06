/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.registry;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          A holder for all registries. 
 * 
 */

public class Registry {
	private EventConsumerRegistry m_consumerRegistry;
	private EventConsumerAffinityRegistry m_primaryAffinityRegistry;
	private EventConsumerAffinityRegistry m_backupAffinityRegistry;
	private EventTopicRegistry m_topicRegistry;
	private WeightRegistry m_weightRegistry;

	/**
	 * @return
	 */
	public EventConsumerRegistry getConsumerRegistry() {
		return m_consumerRegistry;
	}

	/**
	 * @param consumerRegistry
	 */
	public void setConsumerRegistry(EventConsumerRegistry consumerRegistry) {
		this.m_consumerRegistry = consumerRegistry;
	}

	/**
	 * @return
	 */
	public EventConsumerAffinityRegistry getPrimaryAffinityRegistry() {
		return m_primaryAffinityRegistry;
	}

	/**
	 * @param affinityRegistry
	 */
	public void setPrimaryAffinityRegistry(
			EventConsumerAffinityRegistry affinityRegistry) {
		this.m_primaryAffinityRegistry = affinityRegistry;
	}

	/**
	 * @return
	 */
	public EventTopicRegistry getTopicRegistry() {
		return m_topicRegistry;
	}

	/**
	 * @param m_topicRegistry
	 */
	public void setTopicRegistry(EventTopicRegistry m_topicRegistry) {
		this.m_topicRegistry = m_topicRegistry;
	}

	/**
	 * @return
	 */
	public WeightRegistry getWeightRegistry() {
		return m_weightRegistry;
	}

	/**
	 * @param m_weightRegistry
	 */
	public void setWeightRegistry(WeightRegistry m_weightRegistry) {
		this.m_weightRegistry = m_weightRegistry;
	}

	/**
	 * @return
	 */
	public EventConsumerAffinityRegistry getBackupAffinityRegistry() {

		return m_backupAffinityRegistry;
	}

	/**
	 * @param affinityRegistry
	 */
	public void setBackupAffinityRegistry(
			EventConsumerAffinityRegistry affinityRegistry) {
		this.m_backupAffinityRegistry = affinityRegistry;
	}

}
