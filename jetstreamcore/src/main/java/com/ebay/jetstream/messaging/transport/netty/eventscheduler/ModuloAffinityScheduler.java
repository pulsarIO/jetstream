/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventscheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.registry.EventConsumerAffinityRegistry;
import com.ebay.jetstream.messaging.transport.netty.registry.Registry;

/**
 * @author shmurthy - This class is a type of scheduler that implements a modulo algorithm to pick the next consumer that an event
 * needs to be scheduled to. This class is not thread safe and this is by design for performace and also because the transport operates
 * in the context of a single thread for a given context.
 * It maintains a registry of consumers. Each consumer adverstisement comes with a unique consumerid. Every time a consumer is discovered
 * it's consumer id is added to a list and the list is sorted. The list is an arraylist and and index is used to select a consumerid. When
 * an event needs to be scheduled, the affinity key si extracted from the message and a mod is computed of this key using the size of the 
 * list. This gives us an index to lookup the list and get the consumerid. Next this consumerid is used to select a consumer from the 
 * eventconsumer affinity registry for the associated topic. The drawback of this algorithm is that when a consumer is marked dead it is
 * removed from the list changing the list. This causes traffic rebalancing - this algorithm is good if you can tolerate traffic rebalancing
 * on cluster resize. You also tend to get a better distribution of the traffic with this algorithm when the key range is very small.
 *   
 */

public class ModuloAffinityScheduler implements Scheduler {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private final ConcurrentHashMap<JetstreamTopic, EventConsumerAffinityRegistry> m_topicAffinityRegistry = new ConcurrentHashMap<JetstreamTopic, EventConsumerAffinityRegistry>();
	private final ArrayList<Long> m_primaryConsumers = new ArrayList<Long>(); // this is not thread safe - Eventproducer operates in a single thread

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #supportsAffinity()
	 */
	@Override
	public boolean supportsAffinity() {
		// TODO Auto-generated method stub
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #addEventCosumer
	 * (com.ebay.jetstream.messaging.transport.netty.eventproducer
	 * .EventConsumerInfo)
	 */
	@Override
	public void addEventConsumer(EventConsumerInfo info) {

		Long consumerId = info.getAdvertisement().getConsumerId();
		if (!m_primaryConsumers.contains(consumerId)) {
			
			List<JetstreamTopic> topiclist = info.getAdvertisement()
					.getInterestedTopics();

			if (topiclist.isEmpty()) return; // if there are no topics to register we don't need to add this to the scheduler's registry
			
			for (JetstreamTopic topic : topiclist) {
				if (!m_topicAffinityRegistry.containsKey(topic)) {

					m_topicAffinityRegistry.put(topic,
							new EventConsumerAffinityRegistry());

					LOGGER.info(
							"adding consumer @ " + info.getHostAndPort()
									+ " for topic " + topic.toString());

					EventConsumerAffinityRegistry ecar = m_topicAffinityRegistry.get(topic);
					if (ecar != null)
						ecar.put(consumerId, info);
				} else {
					EventConsumerAffinityRegistry ecar = m_topicAffinityRegistry.get(topic);
					if (ecar != null)
						ecar.put(consumerId, info);
				}

			}

			m_primaryConsumers.add(consumerId);
			Collections.sort(m_primaryConsumers); // the assumption is that this
													// class is protected with a
													// mutex.
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #removeEventConsumer
	 * (com.ebay.jetstream.messaging.transport.netty.eventproducer
	 * .EventConsumerInfo)
	 */
	@Override
	public void removeEventConsumer(EventConsumerInfo info) {
		Long consumerId = info.getAdvertisement().getConsumerId();
		if (m_primaryConsumers.contains(consumerId)) {
			
			List<JetstreamTopic> topiclist = info.getAdvertisement()
					.getInterestedTopics();

			for (JetstreamTopic topic : topiclist) {

				if (m_topicAffinityRegistry.containsKey(topic)) {
					EventConsumerAffinityRegistry ecar = m_topicAffinityRegistry.get(topic);
					if (ecar == null) continue;
					ecar.remove(consumerId);
					LOGGER.info(
							"removing consumer @ " + info.getHostAndPort()
									+ " for topic " + topic.toString());

				}

			}

			m_primaryConsumers.remove(consumerId);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #scheduleNext(com.ebay.jetstream.messaging.messagetype.JetstreamMessage,
	 * com.ebay.jetstream.messaging.transport.netty.registry.Registry)
	 */
	@Override
	public EventConsumerInfo scheduleNext(JetstreamMessage msg,
			Registry registry) throws NoConsumerToScheduleException {

		EventConsumerInfo info = null;

		if (m_primaryConsumers.isEmpty())
			return null;

		long key;
		int ndx = 0;

		Object affinityKey = msg.getAffinityKey();

		if (affinityKey instanceof Long) {
			ndx = (int) (((Long) affinityKey).longValue() % m_primaryConsumers
					.size());
		} else {
			ndx = affinityKey.hashCode() % m_primaryConsumers.size();
			ndx = Math.abs(ndx);
		}

		try {
			
			EventConsumerAffinityRegistry ecar = m_topicAffinityRegistry.get(msg.getTopic());
			
			if (ecar != null)
				info = ecar.get(m_primaryConsumers.get(ndx));
		} catch (Throwable t) {
		}

		return info;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #shutdown()
	 */
	@Override
	public void shutdown() {
		m_primaryConsumers.clear();

	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((m_topicAffinityRegistry == null) ? 0
						: m_topicAffinityRegistry.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof ModuloAffinityScheduler)) {
			return false;
		}
		ModuloAffinityScheduler other = (ModuloAffinityScheduler) obj;
		if (m_topicAffinityRegistry == null) {
			if (other.m_topicAffinityRegistry != null) {
				return false;
			}
		} else if (m_topicAffinityRegistry.size() != other.m_topicAffinityRegistry.size()) {
			return false;
		}
		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public  Scheduler clone() throws CloneNotSupportedException  {

		ModuloAffinityScheduler newScheduler = new ModuloAffinityScheduler();

		return newScheduler;

	}

}
