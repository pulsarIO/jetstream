/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.registry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This registry holds information about event consumers bound to a Key (consumerid). This registry is consulted
 *          when forwarding requires affinity to a particular event consumer.
 * 
 */

public class EventConsumerAffinityRegistry implements Externalizable,
		XSerializable {

	private static final long serialVersionUID = 1L;

	private ConcurrentSkipListMap<Long, EventConsumerInfo> m_affinityRegistry = new ConcurrentSkipListMap<Long, EventConsumerInfo>(); //


	private final AtomicLong m_poolSize = new AtomicLong(0);
	
	/**
 * 
 */
	public EventConsumerAffinityRegistry() {
	}

	
	/**
	 * @param key
	 * @return
	 */
	public boolean containsKey(Long key) {
		return m_affinityRegistry.containsKey(key);
	}

	/**
	 * @param ecinfo
	 * @return
	 */
	public boolean containsValue(EventConsumerInfo ecinfo) {
		return m_affinityRegistry.containsValue(ecinfo);
	}

	/**
	 * @param key
	 * @return
	 */
	public EventConsumerInfo get(Long key) {

		return m_affinityRegistry.get(key);
		
	}

	/**
	 * @return
	 */
	public Collection<EventConsumerInfo> getAllEventConsumers() {
		return m_affinityRegistry.values();
	}

	
	/**
	 * @return the poolSize
	 */
	public long getPoolSize() {
		return m_poolSize.get();
	}

	
	

	/**
	 * @param key
	 * @param ecinfo
	 */
	public void put(Long key, EventConsumerInfo ecinfo) {
		if (key != null && ecinfo != null) {

			m_affinityRegistry.put(key, ecinfo);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {

		m_affinityRegistry = (ConcurrentSkipListMap<Long, EventConsumerInfo>) in
				.readObject();
	}

	/**
	 * @param key
	 */
	public void remove(Long key) {
		m_affinityRegistry.remove(key);
	}

	/**
	 * @param poolSize
	 *            the poolSize to set
	 */
	public void setPoolSize(long poolSize) {
		m_poolSize.set(poolSize);
	}

	
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	public void writeExternal(ObjectOutput out) throws IOException {

		out.writeObject(m_affinityRegistry);

	}
}
