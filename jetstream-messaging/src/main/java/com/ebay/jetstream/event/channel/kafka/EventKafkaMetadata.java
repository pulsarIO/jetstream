/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

public class EventKafkaMetadata {

	private static final String DELIMITER = ":";

	private final String topic;
	private final int partition;
	private final long offset;

	public EventKafkaMetadata(String topic, int partition, long offset) {
		this.topic = topic;
		this.partition = partition;
		this.offset = offset;
	}
	
	public String encode() {
		return toString();
	}

	@Override
	public String toString() {
		StringBuffer content = new StringBuffer();
		content.append(offset).append(DELIMITER).append(partition)
				.append(DELIMITER).append(topic);
		return content.toString();
	}

	public static EventKafkaMetadata decodeInstance(String content) {
		int index = content.indexOf(DELIMITER);
		String offsetStr = content.substring(0, index);
		long offset = Long.parseLong(offsetStr);
		content = content.substring(index + 1);
		index = content.indexOf(DELIMITER);
		String partitionStr = content.substring(0, index);
		int partition = Integer.parseInt(partitionStr);
		String topic = content.substring(index + 1);
		return new EventKafkaMetadata(topic, partition, offset);
	}

	public String getTopic() {
		return topic;
	}

	public int getPartition() {
		return partition;
	}

	public long getOffset() {
		return offset;
	}

}
