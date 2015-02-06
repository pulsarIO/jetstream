/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EventKafkaMetadataTest {
	
	@Test
	public void testEncodeAndDecode() {
		String topic = "Topic.test-1";
		int partition = 2;
		long offset = 100;
		
		EventKafkaMetadata meta = new EventKafkaMetadata(topic, partition, offset);
		String encoded = meta.encode();
		
		EventKafkaMetadata decoded = EventKafkaMetadata.decodeInstance(encoded);
		
		assertEquals(topic, decoded.getTopic());
		assertEquals(partition, decoded.getPartition());
		assertEquals(offset, decoded.getOffset());
		
		topic = "Topic:test:1";
		
		meta = new EventKafkaMetadata(topic, partition, offset);
		encoded = meta.encode();
		
		decoded = EventKafkaMetadata.decodeInstance(encoded);
		
		assertEquals(topic, decoded.getTopic());
		assertEquals(partition, decoded.getPartition());
		assertEquals(offset, decoded.getOffset());
	}

}
