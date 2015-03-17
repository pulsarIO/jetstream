/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import com.ebay.jetstream.event.processor.hdfs.util.MiscUtil;

/**
 * @author weifang
 * 
 */
public class PartitionKey {
	private String topic;
	private int partition;

	public PartitionKey(String topic, int partition) {
		this.topic = topic;
		this.partition = partition;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		result = prime * result + partition;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}

		PartitionKey o = (PartitionKey) obj;
		if (!MiscUtil.objEquals(topic, o.topic)) {
			return false;
		}
		return partition == o.partition;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(topic).append("-").append(partition);
		return sb.toString();
	}

}
