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

public class StaticCoordinator extends ZkCoordinator {

	private static final String S_DELIMITER = ",";

	// format: 1,2,3
	private Set<Integer> m_allocated = new HashSet<Integer>();

	public StaticCoordinator(String config, List<String> topics,
			String groupId, String consumerId, KafkaController.ZkConnector zk)
			throws Exception {
		super(topics, groupId, consumerId, zk);
		this.parseAllocated(config);
		this.coordinateAllocatedPartitions();
	}

	@Override
	public synchronized Map<String, Integer> getRebalanceCount() {
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		try {
			for (String topic : m_allTopicPartitions.keySet()) {
				int count = 0;
				int toTake = getToTakePartitions(topic).size();
				if (toTake > 0) {
					count = toTake;
				} else {
					int toRelease = getToReleasePartitions(topic, -1).size();
					if (toRelease > 0)
						count = (-toRelease);
				}
				countMap.put(topic, count);
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
		Set<Integer> myPartitions = getMyPartitions(topic);
		Set<Integer> copy = new HashSet<Integer>(m_allocated);
		copy.removeAll(myPartitions);
		return copy;
	}

	@Override
	public synchronized Set<Integer> getToReleasePartitions(String topic,
			int count) {
		Set<Integer> myPartitions = getMyPartitions(topic);
		Set<Integer> copy = new HashSet<Integer>(myPartitions);
		copy.removeAll(m_allocated);
		return copy;
	}

	private void parseAllocated(String config) {
		if (config == null)
			throw new IllegalArgumentException(
					"Allocated partitions should be specified.");
		String[] strs = config.split(S_DELIMITER);
		if (strs == null || strs.length <= 0)
			return;
		for (String str : strs) {
			try {
				int partition = Integer.parseInt(str.trim());
				if (partition >= 0)
					m_allocated.add(partition);
			} catch (Exception e) {
				LOGGER.error(
						"Illegal partition format " + str.trim());
			}
		}
	}

	// if configured partitions not exist
	private void coordinateAllocatedPartitions() {
		String topic = m_allTopicPartitions.keySet().iterator().next();
		Set<Integer> all = getAllPartitions(topic);
		m_allocated.retainAll(all);
	}

}
