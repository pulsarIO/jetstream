/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import com.ebay.jetstream.event.JetstreamEvent;

public class TestKafkaMessageSerializer implements KafkaMessageSerializer {

	@Override
	public byte[] encodeKey(JetstreamEvent event) {
		return event.toString().getBytes();
	}

	@Override
	public byte[] encodeMessage(JetstreamEvent event) {
		return event.toString().getBytes();
	}

	@Override
	public JetstreamEvent decode(byte[] key, byte[] message) {
		return new JetstreamEvent();
	}

}
