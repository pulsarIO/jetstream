/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.Map;

public interface KafkaConsumer {
	
	void zkReconnected();
	
	void coordinate();

	Map<String, Integer> calcRebalance();

	boolean takePartitions(String topic, int count);

	boolean releasePartitions(String topic, int count);

	void start();

	void stop();
}
