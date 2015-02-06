/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka.test;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.kafka.support.AvroMessageSerializer;

public class AvroMessageSerializerTest {
    @Test
    public void testJson() {
        AvroMessageSerializer serializer = new AvroMessageSerializer();
        JetstreamEvent event = new JetstreamEvent();
        event.setEventType("SOJEvent");
        event.put("Agent", "NotB-o-t");
        event.put("RemoteIP", "222.222.222.222");
        event.put("clickId", "1");
        event.put("pageId", "123");
        event.put("userId", "123");
        event.put("rdt", 0);
        event.put("iframe", 0);
        event.put("kwd", "Haushaltsgeräte");
        JetstreamEvent event2 = serializer.decode(null,
                serializer.encodeMessage(event));
        for (String k : event.keySet()) {
            Assert.assertEquals(event.get(k), event2.get(k));
        }

    }
}
