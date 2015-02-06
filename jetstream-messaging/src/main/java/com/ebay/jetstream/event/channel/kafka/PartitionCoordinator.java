/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.Map;
import java.util.Set;

/**
 * @author xiaojuwu1
 */
public interface PartitionCoordinator {

	void registerConsumer();

	void unregisterConsumer();
	
	Set<Integer> getMyPartitions(String topic);

	Map<String, Integer> getRebalanceCount();

	Set<Integer> getToTakePartitions(String topic);

	Set<Integer> getToReleasePartitions(String topic, int count);

	boolean takePartition(String topic, int partition);

	void releasePartition(String topic, int partition);

	boolean isNewPartition(String topic, int partition);

}
