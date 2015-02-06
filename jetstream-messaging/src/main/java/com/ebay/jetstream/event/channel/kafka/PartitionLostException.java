/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

public class PartitionLostException extends Exception {

	private static final long serialVersionUID = 1L;

	public PartitionLostException(String consumer, String topic, int partition) {
		super(new StringBuffer().append(consumer).append(" lost the owner of ")
				.append(topic).append("-").append(partition)
				.append(" in zookeeper. ").toString());
	}
}
