/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.ebay.jetstream.event.JetstreamEvent;

public class TestKafkaProducer {

	private ProducerConfig config;
	private Producer<byte[], byte[]> producer;

	private String topic;
	private KafkaMessageSerializer serializer;

	public TestKafkaProducer(String topic, String brokerList,
			KafkaMessageSerializer serializer) {
		this.topic = topic;
		this.serializer = serializer;
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("zookeeper.session.timeout.ms", 5000);
		props.put("zookeeper.sync.time.ms", 2000);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
		props.put("compression.codec", "snappy");
		props.put("request.required.acks", "0");
		props.put("producer.type", "async");
		props.put("client.id", "testProducer");

		config = new ProducerConfig(props);
		producer = new Producer<byte[], byte[]>(config);
	}

	public void produce(int count) {
		for (int i = 0; i < count; i++) {
			JetstreamEvent jsEvent = new JetstreamEvent();
			byte[] key = serializer.encodeMessage(jsEvent);
			byte[] message = serializer.encodeMessage(jsEvent);
			producer.send(new KeyedMessage<byte[], byte[]>(topic, key, message));
		}
	}

	public void close() {
		producer.close();
	}

}
