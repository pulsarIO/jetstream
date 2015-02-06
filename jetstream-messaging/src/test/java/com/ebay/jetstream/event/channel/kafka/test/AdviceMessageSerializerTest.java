/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka.test;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.kafka.support.AdvicedEventKafkaSerializer;

public class AdviceMessageSerializerTest {
    @Test
    public void testAdviceSerialization() {
        AdvicedEventKafkaSerializer serializer = new AdvicedEventKafkaSerializer();
        serializer.setUseKryo(false);
        
        AdvicedEventKafkaSerializer kyroSerializer = new AdvicedEventKafkaSerializer();
        kyroSerializer.setUseKryo(true);
        
        JetstreamEvent event = new JetstreamEvent();
        event.setEventType("SOJEvent");
        event.put("Agent", "NotB-o-t");
        event.put("RemoteIP", "222.222.222.222");
        event.put("clickId", "1");
        event.put("pageId", "123");
        event.put("userId", "123");
        event.put("rdt", 0);
        event.put("iframe", 0);
        event.put("kwd", "Haushaltsger?te");
        
        JetstreamEvent event2 = serializer.decode(null,
                serializer.encodeMessage(event));
        for (String k : event.keySet()) {
            Assert.assertEquals(event.get(k), event2.get(k));
        }

        event2 = kyroSerializer.decode(null,
                serializer.encodeMessage(event));
        for (String k : event.keySet()) {
            Assert.assertEquals(event.get(k), event2.get(k));
        }
        
        for (int i = 0; i< 100; i++) {
            event.put("acb", i);
            event2 = kyroSerializer.decode(null,
                    serializer.encodeMessage(event));
            for (String k : event.keySet()) {
                Assert.assertEquals(event.get(k), event2.get(k));
            }
        }
        
        for (int i = 0; i< 100; i++) {
            event.put("acb", i);
            event2 = kyroSerializer.decode(null,
                    kyroSerializer.encodeMessage(event));
            for (String k : event.keySet()) {
                Assert.assertEquals(event.get(k), event2.get(k));
            }
        }
    }
}