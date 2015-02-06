/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

/**
 * @author xiaojuwu1
 */
public class BatchSource {

	private String topic;
	private Object partition;
	private long headOffset;

	public BatchSource() {
	}

	public BatchSource(String topic, Object partition) {
		this.topic = topic;
		this.partition = partition;
	}
	
	public BatchSource(String topic, Object partition, long offset) {
		this.topic = topic;
		this.partition = partition;
		this.headOffset = offset;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Object getPartition() {
		return partition;
	}

	public void setPartition(Object partition) {
		this.partition = partition;
	}

	public long getHeadOffset() {
		return headOffset;
	}

	public void setHeadOffset(long headOffset) {
		this.headOffset = headOffset;
	}

}
