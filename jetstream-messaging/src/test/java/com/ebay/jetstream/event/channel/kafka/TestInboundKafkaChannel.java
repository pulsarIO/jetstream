/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.mockito.Mockito.mock;

public class TestInboundKafkaChannel extends InboundKafkaChannel {

	@Override
	public PartitionReader createPartitionReader(String topic, int partition) {
		PartitionReader reader = mock(PartitionReader.class);
		return reader;
	}
}
