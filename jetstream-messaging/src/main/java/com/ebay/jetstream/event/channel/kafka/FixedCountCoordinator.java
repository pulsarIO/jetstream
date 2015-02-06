/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author xiaojuwu1
 */
public class FixedCountCoordinator extends ZkCoordinator {

	private final int m_fixedCount;

	public FixedCountCoordinator(int fixedCount, List<String> topics,
			String groupId, String consumerId, KafkaController.ZkConnector zk)
			throws Exception {
		super(topics, groupId, consumerId, zk);
		this.m_fixedCount = fixedCount;
	}

	@Override
	public synchronized Map<String, Integer> getRebalanceCount() {
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		try {
			for (String topic : m_allTopicPartitions.keySet()) {
				Set<Integer> myPartitions = getMyPartitions(topic);
				countMap.put(topic, m_fixedCount - myPartitions.size());
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
		return getIdlePartitions(topic);
	}

	@Override
	public synchronized Set<Integer> getToReleasePartitions(String topic,
			int count) {
		return getRandomReleasePartitions(topic, count);
	}
}
