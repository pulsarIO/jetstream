/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xiaojuwu1
 */
public abstract class ZkCoordinator implements PartitionCoordinator {

	protected static final Logger LOGGER = LoggerFactory.getLogger(ZkCoordinator.class
			.getName());

	protected final String m_groupId;
	protected final String m_consumerId;

	protected final String m_groupPath;
	protected final String m_consumerPath;
	protected final String m_ownerPath;
	protected final String m_offsetPath;

	protected KafkaController.ZkConnector m_zk;

	protected Map<String, Set<Integer>> m_allTopicPartitions = new HashMap<String, Set<Integer>>();
	protected Map<String, Set<Integer>> m_myTopicPartitions = new HashMap<String, Set<Integer>>();

	/**
	 * keep all the partitions which are newly added after channel is started
	 */
	protected Map<String, Set<Integer>> m_newTopicPartitions = new HashMap<String, Set<Integer>>();

	public ZkCoordinator(List<String> topics, String groupId,
			String consumerId, KafkaController.ZkConnector zk) throws Exception {

		this.m_groupId = groupId;
		this.m_consumerId = consumerId;
		this.m_zk = zk;

		if (topics != null && !topics.isEmpty()) {
			for (String topic : topics) {
				m_allTopicPartitions.put(topic, new HashSet<Integer>());
				m_myTopicPartitions.put(topic, new HashSet<Integer>());
			}
		}

		this.m_groupPath = groupPath();
		this.m_consumerPath = consumerPath();
		this.m_ownerPath = ownerPath();
		this.m_offsetPath = offsetPath();
	}

	@Override
	public synchronized void registerConsumer() {
		try {
			if (!m_zk.exists(m_groupPath)) {
				if (m_zk.create(m_groupPath, false)) {
					m_zk.create(m_consumerPath, false);
					m_zk.create(m_offsetPath, false);
					m_zk.create(m_ownerPath, false);
				}
			}
			m_zk.create(consumerPath(m_consumerId), true);

			if (LOGGER.isInfoEnabled())
				LOGGER.info( "Register consumer " + m_consumerId
						+ " to zookeeper.");

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void unregisterConsumer() {
		try {
			String path = consumerPath(m_consumerId);
			if (m_zk.exists(path))
				m_zk.delete(path);

			if (LOGGER.isInfoEnabled())
				LOGGER.info( "Unegister consumer " + m_consumerId
						+ " to zookeeper.");

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void reRegisterConsumer(boolean force) {
		try {
			String path = consumerPath(m_consumerId);
			if (force) {
				m_zk.delete(path);
				m_zk.create(path, true);
			} else {
				if (!m_zk.exists(path)) {
					m_zk.create(path, true);
				} else
					return;
			}

			if (LOGGER.isInfoEnabled())
				LOGGER.info( "ReRegister consumer " + m_consumerId
						+ " to zookeeper.");

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized boolean isNewPartition(String topic, int partition) {
		StringBuffer sb = new StringBuffer(m_offsetPath);
		sb.append("/").append(topic).append("/").append(partition);
		boolean isOffsetExist = m_zk.exists(sb.toString());

		boolean newAdd = false;
		Set<Integer> partitions = m_newTopicPartitions.get(topic);
		if (partitions != null && partitions.contains(partition))
			newAdd = true;

		return newAdd && !isOffsetExist;
	}

	/**
	 * take the partition in zookeeper, if taken(not idle), return false
	 */
	@Override
	public synchronized boolean takePartition(String topic, int partition) {
		Set<Integer> partitionSet = m_myTopicPartitions.get(topic);
		if (partitionSet == null)
			return false;

		String ownerPath = ownerPath(topic, partition);
		try {
			if (m_zk.exists(ownerPath)) {
				return false;
			}
			if (m_zk.create(ownerPath, true)) {
				m_zk.writeString(ownerPath, m_consumerId);
				partitionSet.add(partition);
				return true;
			} else
				return false;

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			LOGGER.error( "Fail to take partition " + ownerPath, e);
			return false;
		}
	}

	public synchronized boolean resetOwner(String topic, int partition) {
		String ownerPath = ownerPath(topic, partition);
		try {
			if (m_zk.exists(ownerPath)) {
				m_zk.delete(ownerPath);
			}
			if (m_zk.create(ownerPath, true)) {
				if (LOGGER.isInfoEnabled())
					LOGGER.info( "Owner path " + ownerPath
							+ " is rewritten.");
				m_zk.writeString(ownerPath, m_consumerId);
				return true;
			} else
				return false;

		} catch (Exception e) {
			LOGGER.error( "Fail to reset owner " + ownerPath, e);
			return false;
		}
	}

	/**
	 * release taken partition
	 */
	@Override
	public synchronized void releasePartition(String topic, int partition) {
		Set<Integer> partitionSet = m_myTopicPartitions.get(topic);
		if (partitionSet == null)
			return;
		String ownerPath = ownerPath(topic, partition);
		try {
			if (m_zk.exists(ownerPath)
					&& m_consumerId.equals(m_zk.readString(ownerPath))) {
				m_zk.delete(ownerPath);
			}
			partitionSet.remove(partition);
		} catch (Exception e) {
			LOGGER.error( m_consumerId
					+ "Fail to release partition " + ownerPath, e);
		}
	}

	/**
	 * get partitions taken by this consumer
	 */
	@Override
	public synchronized Set<Integer> getMyPartitions(String topic) {
		Set<Integer> partitions = new HashSet<Integer>();
		try {
			String ownerPath = ownerPath(topic);
			if (!m_zk.exists(ownerPath))
				return partitions;

			List<String> pstrs = m_zk.getChildren(ownerPath);
			if (pstrs == null || pstrs.isEmpty())
				return partitions;

			for (String pstr : pstrs) {
				int pid = Integer.parseInt(pstr);
				// double check for data may change since last check
				String path = ownerPath(topic, pid);
				if (!m_zk.exists(path))
					continue;
				String ownerId = m_zk.readString(path);
				if (ownerId != null && ownerId.equals(m_consumerId)) {
					partitions.add(pid);
				}
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		m_myTopicPartitions.put(topic, new HashSet<Integer>(partitions));
		return partitions;
	}

	public synchronized boolean isZkOwnerExist(String topic, int partition) {
		String ownerPath = ownerPath(topic, partition);
		try {
			if (m_zk.exists(ownerPath)
					&& m_consumerId.equals(m_zk.readString(ownerPath))) {
				return true;
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return false;
	}

	protected synchronized Set<Integer> getAllPartitions(String topic) {
		Set<Integer> partitions = new HashSet<Integer>();
		try {
			String path = partitionPath(topic);
			// if topic not exist in zookeeper, skip
			if (!m_zk.exists(path)) {
				LOGGER.warn( "Node " + path
						+ " does not exist in zookeeper.");
				return partitions;
			}
			List<String> pstrs = m_zk.getChildren(path);
			if (pstrs == null || pstrs.isEmpty()) {
				LOGGER.warn( "No partitions available for topic "
						+ topic);
				return partitions;
			}
			for (String pstr : pstrs) {
				int pid = Integer.parseInt(pstr);
				partitions.add(pid);
			}
		} catch (Exception e) {
			LOGGER.error( "Error occurs in getting partitions of "
					+ topic, e);
		}

		findNewlyAddPartitions(topic, partitions);

		m_allTopicPartitions.put(topic, new HashSet<Integer>(partitions));
		return partitions;
	}

	// find newly add partitions
	private void findNewlyAddPartitions(String topic, Set<Integer> newAll) {
		Set<Integer> oldSet = m_allTopicPartitions.get(topic);
		Set<Integer> copy = new HashSet<Integer>(newAll);
		copy.removeAll(oldSet);
		if (!copy.isEmpty()) {
			Set<Integer> newlyAdd = m_newTopicPartitions.get(topic);
			if (newlyAdd == null) {
				newlyAdd = new HashSet<Integer>();
				m_newTopicPartitions.put(topic, newlyAdd);
			}
			newlyAdd.addAll(copy);
		}
	}

	protected synchronized Set<Integer> getIdlePartitions(String topic) {
		try {
			Set<Integer> allPartitions = m_allTopicPartitions.get(topic);
			if (allPartitions == null || allPartitions.isEmpty())
				allPartitions = getAllPartitions(topic);

			Set<Integer> idle = new HashSet<Integer>(allPartitions);
			for (int partition : allPartitions) {
				if (m_zk.exists(ownerPath(topic, partition)))
					idle.remove(partition);
			}
			return idle;

		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected synchronized Set<Integer> getRandomReleasePartitions(
			String topic, int count) {
		try {
			Set<Integer> toRelease = new HashSet<Integer>();
			Set<Integer> myPartitions = m_myTopicPartitions.get(topic);
			if (myPartitions == null || myPartitions.size() < count)
				return toRelease;

			Iterator<Integer> it = myPartitions.iterator();
			while (it.hasNext() && toRelease.size() < count) {
				toRelease.add(it.next());
			}
			return toRelease;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected synchronized List<String> getAllConsumers() {
		return m_zk.getChildren(m_consumerPath);
	}

	protected String groupPath() {
		StringBuffer sb = new StringBuffer("/consumers/");
		sb.append(m_groupId);
		return sb.toString();
	}

	protected String consumerPath() {
		StringBuffer sb = new StringBuffer("/consumers/");
		sb.append(m_groupId).append("/ids");
		return sb.toString();
	}

	protected String consumerPath(String consumerId) {
		StringBuffer sb = new StringBuffer("/consumers/");
		sb.append(m_groupId).append("/ids");
		sb.append("/").append(consumerId);
		return sb.toString();
	}

	protected String ownerPath() {
		StringBuffer sb = new StringBuffer("/consumers/");
		sb.append(m_groupId).append("/owners");
		return sb.toString();
	}

	protected String ownerPath(String topic) {
		StringBuffer sb = new StringBuffer("/consumers/");
		sb.append(m_groupId).append("/owners/");
		sb.append(topic);
		return sb.toString();
	}

	protected String ownerPath(String topic, int partition) {
		StringBuffer sb = new StringBuffer("/consumers/");
		sb.append(m_groupId).append("/owners/");
		sb.append(topic).append("/").append(partition);
		return sb.toString();
	}

	protected String offsetPath() {
		StringBuffer sb = new StringBuffer("/consumers/");
		sb.append(m_groupId).append("/offsets");
		return sb.toString();
	}

	protected String partitionPath(String topic) {
		StringBuffer sb = new StringBuffer("/brokers/topics/");
		sb.append(topic).append("/partitions");
		return sb.toString();
	}

	protected int getConsumerIndex(List<String> allConsumers) {
		Collections.sort(allConsumers);
		for (int i = 0; i < allConsumers.size(); i++) {
			if (allConsumers.get(i).equals(m_consumerId)) {
				return i;
			}
		}
		throw new RuntimeException(
				"Fatal: could not find this consumer id in zookeeper's consumer list.");
	}

}
