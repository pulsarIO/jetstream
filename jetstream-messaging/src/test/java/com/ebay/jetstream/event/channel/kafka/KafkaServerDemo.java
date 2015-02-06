/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaServerDemo {

	public static void main(String[] args) {
		TestZookeeperServer zkServer;
		try {
			zkServer = new TestZookeeperServer(30000, 2183, 100);
			zkServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}

		String zkConnect = "localhost:2183";
		TestKafkaServer kafkaBroker0 = new TestKafkaServer(
				"/tmp/embedded/kafka0/", 9092, 0, zkConnect, 1);

		TestKafkaMessageSerializer serializer = new TestKafkaMessageSerializer();

		String topic = "Topic.test-1";
		TestKafkaProducer producer = new TestKafkaProducer(topic,
				"localhost:9092", serializer);
		producer.produce(100);

		String clientId = "test_demo";
		SimpleConsumer consumer = new SimpleConsumer("localhost", 9092, 30000,
				65536, clientId);

		FetchRequest req = new FetchRequestBuilder().clientId(clientId)
				.addFetch(topic, 0, 0, 10).build();

		FetchResponse fetchResponse = null;
		try {
			fetchResponse = consumer.fetch(req);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (fetchResponse != null) {
			if (fetchResponse.hasError()) {
				short errorCode = fetchResponse.errorCode(topic, 0);
				System.out.println(errorCode);
				if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
					System.out.println("Unkown topic partition");
				}
			}
		}
	}

}
