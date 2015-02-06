/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author xiaojuwu1
 */
public class DynamicCoordinator extends ZkCoordinator {

	private int m_thisConsumerIndex = -1;

	public DynamicCoordinator(List<String> topics, String groupId,
			String consumerId, KafkaController.ZkConnector zk) throws Exception {
		super(topics, groupId, consumerId, zk);
	}

	/**
	 * @return rt==0 do nothing rt>0 should take more partitions rt<0 should
	 *         release some partitions
	 */
	@Override
	public synchronized Map<String, Integer> getRebalanceCount() {
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		try {
			List<String> allConsumers = getAllConsumers();
			m_thisConsumerIndex = getConsumerIndex(allConsumers);
			int consumerCount = allConsumers.size();
			if (consumerCount <= 0) {
				LOGGER.error( "No consumers registered at node  "
						+ m_consumerPath);
				return countMap;
			}

			for (String topic : m_allTopicPartitions.keySet()) {
				Set<Integer> oldAll = m_allTopicPartitions.get(topic);
				Set<Integer> newAll = getAllPartitions(topic);

				// find the newly added partitions
				if (oldAll != null && newAll != null) {
					Set<Integer> newOnes = new HashSet<Integer>(newAll);
					newOnes.removeAll(oldAll);
					Set<Integer> newPartitions = m_newTopicPartitions.get(topic);
					if (newPartitions != null && !newOnes.isEmpty()) {
						newPartitions.addAll(newOnes);
					}
				}

				// calc rebalance count
				int partitionCount = newAll.size();
				int minTake = partitionCount / consumerCount;
				int left = partitionCount % consumerCount;
				int shouldTake = minTake;
				if (m_thisConsumerIndex < left)
					shouldTake++;

				Set<Integer> myPartitions = getMyPartitions(topic);
				int takenCount = myPartitions.size();
				int rebalanceCount = shouldTake - takenCount;
				countMap.put(topic, rebalanceCount);
			}

			return countMap;

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized Set<Integer> getToTakePartitions(String topic) {
		if (!m_allTopicPartitions.containsKey(topic))
			return new HashSet<Integer>();
		return getIdlePartitions(topic);
	}

	@Override
	public synchronized Set<Integer> getToReleasePartitions(String topic,
			int count) {
		return getRandomReleasePartitions(topic, count);
	}

}
