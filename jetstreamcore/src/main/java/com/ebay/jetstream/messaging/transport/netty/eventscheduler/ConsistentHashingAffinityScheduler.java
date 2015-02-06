/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventscheduler;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.ClassLoader;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.registry.Registry;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing.ConsistentHashing;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing.DefaultHashFunction;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing.HashFunction;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy - This class is a type of scheduler that implements a consistent hashing algorithm. This class is not thread safe.
 * It is designed this way for performance and also because the netty transport is single threaded per context. It maintains a registry of
 * several hashes per event consumer per topic. It selects the consumer to which the event needs to be scheduled applying a consistent hashing
 * algorithm. This algorithm works very well when the affinity key range is very well spread but does not work well when the range is small.
 * When a node goes down this algorithm balances traffic destined for that node across the cluster without affecting traffic going to other nodes.
 * The rebalancing spreads the traffic around instead of moving it to its neighbors.
 */


@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"}) 
public class ConsistentHashingAffinityScheduler implements Scheduler,
		XSerializable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private final ConcurrentHashMap<JetstreamTopic, ConsistentHashing<EventConsumerInfo>> m_ch = new ConcurrentHashMap<JetstreamTopic, ConsistentHashing<EventConsumerInfo>>();

	private int m_numHashes = 5000;
	private long m_spreadFactor = 987654321098L;
	private final Map<String, LongCounter> m_counters = new ConcurrentHashMap<String, LongCounter>();
	private HashFunction m_hashFunction;
	private String m_hashFunctionClass = "com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing.DefaultHashFunction";
	private ConsistentHashingRingUpdateListener m_updateListener;
	private Map<Long, EventConsumerInfo> m_comsumerInfoMap = new HashMap<Long, EventConsumerInfo>();



	
	public ConsistentHashingRingUpdateListener getUpdateListener() {
		return m_updateListener;
	}


	public void setUpdateListener(
			ConsistentHashingRingUpdateListener updateListener) {
		this.m_updateListener = updateListener;
	}


	/**
	 * @return
	 */
	public String getHashFunctionClass() {
		return m_hashFunctionClass;
	}


	/**
	 * @param hashFunctionClass
	 */
	public void setHashFunctionClass(String hashFunctionClass) {
		m_hashFunctionClass = hashFunctionClass;
		m_hashFunction = (HashFunction) ClassLoader.load(hashFunctionClass);
		
		if (m_hashFunction == null) {
			LOGGER.error( "failed to instantiate class - " + hashFunctionClass);
			try {
				m_hashFunction = new DefaultHashFunction();
			} catch (NoSuchAlgorithmException e) {
				LOGGER.error( "failed to instantiate DefaultHashFunction");
		
			} 
		}
	}

	
	/**
	 * @return
	 */
	public HashFunction getHashFunction() {
		return m_hashFunction;
	}

	
	/**
	 * @return
	 */
	public Map<String, LongCounter> getCounters() {
		return Collections.unmodifiableMap(m_counters);
	}

	/**
	 * @return
	 */
	public int getNumHashes() {
		return m_numHashes;
	}

	/**
	 * @param m_numHashes
	 */
	public void setNumHashes(int m_numHashes) {
		this.m_numHashes = m_numHashes;
	}

	/**
	 * @return
	 */
	public long getSpreadFactor() {
		return m_spreadFactor;
	}

	/**
	 * @param m_spreadFactor
	 */
	public void setSpreadFactor(long m_spreadFactor) {
		this.m_spreadFactor = m_spreadFactor;
	}

	/**
	 * 
	 */
	public ConsistentHashingAffinityScheduler() {
		
		try {
			m_hashFunction = new DefaultHashFunction();
		} catch (NoSuchAlgorithmException e) {
			LOGGER.error( "failed to instantiate DefaultHashFunction");
	
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

		try {
			if (m_ch.containsKey(msg.getTopic())) {
				ConsistentHashing<EventConsumerInfo> che = m_ch.get(msg.getTopic());
				Object obj = msg.getAffinityKey();
				if ((che != null) && (obj != null)) 
					info = che.get(obj);
			}
			if (info != null)
				m_counters.get(info.getHostAndPort()).increment();
		} catch (Throwable t) {}
		
		return info;

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

		List<JetstreamTopic> topiclist = info.getAdvertisement()
				.getInterestedTopics();

		m_comsumerInfoMap.put(info.getAdvertisement().getConsumerId(), info);
		
		for (JetstreamTopic topic : topiclist) {
			if (!m_ch.containsKey(topic)) {
				
					m_ch.put(topic, new ConsistentHashing<EventConsumerInfo>(
							m_hashFunction, m_numHashes,
							m_spreadFactor));

					
				ConsistentHashing<EventConsumerInfo> che = m_ch.get(topic);
				if ((che != null) && (topic != null) && (info != null)) {
					m_ch.get(topic).add(info);
					rehashCHRing(che, info, topic);
					LOGGER.info(
							"adding consumer @ " + info.getHostAndPort()
									+ " for topic " + topic.toString());
				}
			} else {
				ConsistentHashing<EventConsumerInfo> che = m_ch.get(topic);
				if (che != null) che.add(info);
				rehashCHRing(che, info, topic);
			}

		}
		
		m_counters.put(info.getHostAndPort(), new LongCounter());

		if (m_updateListener != null)
			m_updateListener.update(Collections.unmodifiableMap(m_ch), Collections.unmodifiableMap(m_comsumerInfoMap));
		
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

		List<JetstreamTopic> topiclist = info.getAdvertisement()
				.getInterestedTopics();

		m_comsumerInfoMap.remove(info.getAdvertisement().getConsumerId());
		
		for (JetstreamTopic topic : topiclist) {

			if (m_ch.containsKey(topic)) {
				ConsistentHashing<EventConsumerInfo> che = m_ch.get(topic);
				LOGGER.info(
						"removing consumer @ " + info.getHostAndPort()
								+ " for topic " + topic.toString());

				if (che != null) {
					che.remove(info);
					rehashCHRing(che, info, topic);
				}
				
				if (che.isEmpty()) {
					m_ch.remove(topic);
				}
			}

		}
		
		m_counters.remove(info.getHostAndPort());
		
		if (m_updateListener != null)
			m_updateListener.update(Collections.unmodifiableMap(m_ch), Collections.unmodifiableMap(m_comsumerInfoMap));

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

		m_ch.clear();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.transport.netty.eventscheduler.Scheduler
	 * #supportsAffinity()
	 */
	@Override
	public boolean supportsAffinity() {

		return true;
	}
	

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + m_numHashes;
		result = prime * result
				+ (int) (m_spreadFactor ^ (m_spreadFactor >>> 32));
		return result;
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

		if (!(obj instanceof ConsistentHashingAffinityScheduler))
			return false;

		ConsistentHashingAffinityScheduler cc = (ConsistentHashingAffinityScheduler) obj;

		if (m_numHashes != cc.getNumHashes())
			return false;
		
		if (m_spreadFactor != cc.getSpreadFactor())
			return false;

		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	
	public  Scheduler clone() throws CloneNotSupportedException  {
		
		ConsistentHashingAffinityScheduler newScheduler = new ConsistentHashingAffinityScheduler();
		
		newScheduler.m_hashFunction = this.m_hashFunction;
		newScheduler.m_numHashes = this.m_numHashes;
		newScheduler.m_hashFunctionClass = this.m_hashFunctionClass;
		newScheduler.m_spreadFactor = this.m_spreadFactor;
		newScheduler.m_updateListener = this.m_updateListener;
		
		return newScheduler;
	
	}
	
	/**
	 * Needed to recompute all the hashes in the ring so all nodes have same view of the ring
	 * @param che
	 * @param info
	 */
	private void rehashCHRing(ConsistentHashing<EventConsumerInfo> che, EventConsumerInfo info, JetstreamTopic topic) {
		List<Long> consumerIds = new ArrayList<Long>(m_comsumerInfoMap.keySet());
		Collections.sort(consumerIds);
		for (int i = 0; i < consumerIds.size(); i++) {
			Long c = consumerIds.get(i);
			if (m_comsumerInfoMap.get(c).getAdvertisement().getInterestedTopics().contains(topic)) {
				che.add(m_comsumerInfoMap.get(c));
			}
		}
	}


}
