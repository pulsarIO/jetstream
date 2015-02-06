/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * This is the config bean for the kafka consumer.
 * 
 * Below is a configuration example.
 * 
 * 
 * @author xingwang, weifang, xiaojuwu1
 * 
 */
public class KafkaConsumerConfig extends AbstractNamedBean implements
		XSerializable {

	private boolean enabled;
	private int poolSize = 5;
	private boolean subscribeOnInit = true; // Means the consumer auto
											// subscriber during init or not.
	private int batchSizeBytes = 1024 * 1024;
	private int maxBatchSizeBytes = 2 * 1024 * 1024;

	private boolean dynamicAllocatePartition = true;
	private int fixedPartitionCountPerTopic = 0;
	private String allocatedPartitions;

	private String groupId;
	private int rebalanceMaxRetries = 4;
	private String autoOffsetReset = "largest";
	private int fetchWaitMaxMs = 1000;
	private int socketTimeoutMs = 30000;
	private int socketReceiveBufferBytes = 65536;

	private int idleTimeInMs = 60000;

	public boolean getEnabled() {
		return enabled;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public boolean getSubscribeOnInit() {
		return subscribeOnInit;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public void setSubscribeOnInit(boolean subscribeOnInit) {
		this.subscribeOnInit = subscribeOnInit;
	}

	public int getBatchSizeBytes() {
		return batchSizeBytes;
	}

	public void setBatchSizeBytes(int batchSizeBytes) {
		this.batchSizeBytes = batchSizeBytes;
	}

	public int getMaxBatchSizeBytes() {
		return maxBatchSizeBytes;
	}

	public void setMaxBatchSizeBytes(int maxBatchSizeBytes) {
		this.maxBatchSizeBytes = maxBatchSizeBytes;
	}

	public boolean isDynamicAllocatePartition() {
		return dynamicAllocatePartition;
	}

	public void setDynamicAllocatePartition(boolean dynamicAllocatePartition) {
		this.dynamicAllocatePartition = dynamicAllocatePartition;
	}

	public int getFixedPartitionCountPerTopic() {
		return fixedPartitionCountPerTopic;
	}

	public void setFixedPartitionCountPerTopic(int fixedPartitionCount) {
		this.fixedPartitionCountPerTopic = fixedPartitionCount;
	}

	public String getAllocatedPartitions() {
		return allocatedPartitions;
	}

	public void setAllocatedPartitions(String allocatedPartitions) {
		this.allocatedPartitions = allocatedPartitions;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public int getRebalanceMaxRetries() {
		return rebalanceMaxRetries;
	}

	public void setRebalanceMaxRetries(int rebalanceMaxRetries) {
		this.rebalanceMaxRetries = rebalanceMaxRetries;
	}

	public String getAutoOffsetReset() {
		return autoOffsetReset;
	}

	public void setAutoOffsetReset(String autoOffsetReset) {
		this.autoOffsetReset = autoOffsetReset;
	}

	public int getFetchWaitMaxMs() {
		return fetchWaitMaxMs;
	}

	public void setFetchWaitMaxMs(int fetchWaitMaxMs) {
		this.fetchWaitMaxMs = fetchWaitMaxMs;
	}

	public int getSocketTimeoutMs() {
		return socketTimeoutMs;
	}

	public void setSocketTimeoutMs(int socketTimeoutMs) {
		this.socketTimeoutMs = socketTimeoutMs;
	}

	public int getSocketReceiveBufferBytes() {
		return socketReceiveBufferBytes;
	}

	public void setSocketReceiveBufferBytes(int socketReceiveBufferBytes) {
		this.socketReceiveBufferBytes = socketReceiveBufferBytes;
	}

	public int getIdleTimeInMs() {
		return idleTimeInMs;
	}

	public void setIdleTimeInMs(int idleTimeInMs) {
		this.idleTimeInMs = idleTimeInMs;
	}

}
