/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka.test;

import static kafka.api.OffsetRequest.EarliestTime;
import static kafka.api.OffsetRequest.LatestTime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class SimpleConsumerTest {

	private static String leader = "phx8b03c-347a.stratus.phx.qa.ebay.com";
	private static String topic = "Replay-Trkng.distributor-ssnzEvent";
	private static int partition = 3;
//	private static long offset = 5787645L; 
	private static long offset = 12296213L;
	private static String clientId = "testClient";

	public static void main(String[] args) {
		SimpleConsumer consumer = new SimpleConsumer(leader, 9092, 30000,
				65536, clientId);
		
		// fetch earliest offset in kafka
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(new TopicAndPartition(topic, partition),
				new PartitionOffsetRequestInfo(EarliestTime(), 1));
		OffsetRequest request = new OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), clientId);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			System.out.println(response.errorCode(topic, partition));
		} else {
			long[] offsets = response.offsets(topic, partition);
			System.out.println("Earliest offset " + offsets[0]);
		}
		
		// fetch latest offset in kafka
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo2 = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo2.put(new TopicAndPartition(topic, partition),
				new PartitionOffsetRequestInfo(LatestTime(), 1));
		OffsetRequest request2 = new OffsetRequest(requestInfo2,
				kafka.api.OffsetRequest.CurrentVersion(), clientId);
		OffsetResponse response2 = consumer.getOffsetsBefore(request2);
		if (response2.hasError()) {
			System.out.println(response2.errorCode(topic, partition));
		} else {
			long[] offsets = response2.offsets(topic, partition);
			System.out.println("Latest offset " + offsets[0]);
		}
		
		
		FetchRequest req = new FetchRequestBuilder().clientId(clientId)
				.addFetch(topic, partition, offset, 1638400).build();
		FetchResponse fetchResponse = consumer.fetch(req);
		if (fetchResponse.hasError()) {
			System.out.println("errorCode="
					+ fetchResponse.errorCode(topic, partition));
		} else {
			ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic,
					partition);

			Iterator<MessageAndOffset> it = messageSet.iterator();
			if (it.hasNext()) {
				System.out.println("have message");
			}
			long msgCount = 0;
			long minOffset = Long.MAX_VALUE;
			long maxOffset = 0L;
			for (MessageAndOffset messageAndOffset : messageSet) {
				msgCount++;
				long offset = messageAndOffset.offset();
				if (minOffset > offset) 
					minOffset = offset;
				if (maxOffset < offset) 
					maxOffset = offset;
			}
			System.out.println("msgCount=" + msgCount);
			System.out.println("minOffset=" + minOffset);
			System.out.println("maxOffset=" + maxOffset);
			System.out.println("readOffset=" + offset);
		}

		
	}

}
