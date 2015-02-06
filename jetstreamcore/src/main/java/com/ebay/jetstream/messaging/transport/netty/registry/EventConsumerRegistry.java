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
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This registry holds information about event consumers bound to a host-port
 * 
 */
public class EventConsumerRegistry implements Externalizable, XSerializable {

	private static final long serialVersionUID = 1L;

	private ConcurrentHashMap<String, EventConsumerInfo> m_eventConsumerRegistry = new ConcurrentHashMap<String, EventConsumerInfo>(); // key
																																		// =
																																		// hostAndPort

	/**
	 * @return
	 */
	public ConcurrentHashMap<String, EventConsumerInfo> getEventConsumerRegistry() {
		return m_eventConsumerRegistry;
	}

	/**
	 * @param info
	 */
	public void add(EventConsumerInfo info) {

		if (!m_eventConsumerRegistry.containsKey(info.getAdvertisement()
				.getHostAndPort()))
			m_eventConsumerRegistry.put(info.getAdvertisement()
					.getHostAndPort(), info);

	}

	/**
	 * @param info
	 */
	public void remove(EventConsumerInfo info) {
		m_eventConsumerRegistry
				.remove(info.getAdvertisement().getHostAndPort());

	}

	/**
	 * @param advertisement
	 * @return
	 */
	public EventConsumerInfo get(EventConsumerAdvertisement advertisement) {

		if (m_eventConsumerRegistry.containsKey(advertisement.getHostAndPort()))
			return m_eventConsumerRegistry.get(advertisement.getHostAndPort());
		else
			return null;

	}

	/**
	 * @param hostAndPort
	 * @return
	 */
	public EventConsumerInfo get(String hostAndPort) {

		if (m_eventConsumerRegistry.containsKey(hostAndPort))
			return m_eventConsumerRegistry.get(hostAndPort);
		else
			return null;

	}

	/**
	 * @param advertisement
	 * @return
	 */
	public boolean hasConsumerWithThisAdvertisement(
			EventConsumerAdvertisement advertisement) {
		return m_eventConsumerRegistry.containsKey(advertisement
				.getHostAndPort());
	}

	
	/**
	 * @return
	 */
	public Enumeration<EventConsumerInfo> getConsumers() {
		return m_eventConsumerRegistry.elements();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {

		out.writeObject(m_eventConsumerRegistry);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		m_eventConsumerRegistry = (ConcurrentHashMap<String, EventConsumerInfo>) in
				.readObject();

	}

	public Enumeration<EventConsumerInfo> getAllConsumers() {

		return m_eventConsumerRegistry.elements();
		
	}
	
	public boolean isEmpty() {
		return m_eventConsumerRegistry.isEmpty();
	}
}
