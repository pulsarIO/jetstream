/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;

public class OffHeapMapTest {
    @Test
    public void testMap() {
        OffHeapSerializer<String> strSerializer = DefaultSerializerFactory.getInstance().getStringSerializer();
        MapBuilder<String, String> builder = MapBuilder.newBuilder();
        Map<String, String> map = builder.withKeySerializer(strSerializer).withValueSerializer(strSerializer).buildHashMap();
        
        int count = 10000;
        for (int i = 0; i < count; i++) {
            map.put("Key" + i, "Value"+ i);
        }
        Assert.assertEquals(map.size(), count);
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.get("Key" + i), "Value"+ i);
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.put("Key" + i, "Value"+ (i * 2)), "Value"+ i);
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.get("Key" + i), "Value"+ (i * 2));
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.put("Key" + i, "Value"+ i), "Value"+ (i * 2));
        }
        
        Assert.assertEquals(map.keySet().size(), count);
        Assert.assertEquals(map.values().size(), count);
        Assert.assertEquals(map.entrySet().size(), count);
        Assert.assertEquals(map.size(), count);
        map.clear();
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
        Assert.assertTrue(map.keySet().isEmpty());
        Assert.assertTrue(map.values().isEmpty());
        Assert.assertTrue(map.entrySet().isEmpty());
        Assert.assertEquals(map.size(), 0);
        
        for (int i = 0; i < count; i++) {
            map.put("Key" + i, "Value"+ i);
        }
        Assert.assertEquals(map.size(), count);
        
        for (int i = 0; i < count; i++) {
            map.remove("Key" + i);
        }
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
        Assert.assertTrue(map.keySet().isEmpty());
        Assert.assertTrue(map.values().isEmpty());
        Assert.assertTrue(map.entrySet().isEmpty());
        Assert.assertEquals(map.size(), 0);
    }
    
    @Test
    public void testConcurrentMap() {
        OffHeapSerializer<String> strSerializer = DefaultSerializerFactory.getInstance().getStringSerializer();
        MapBuilder<String, String> builder = MapBuilder.newBuilder();
        ConcurrentMap<String, String> map = builder.withKeySerializer(strSerializer).withValueSerializer(strSerializer).buildConcurrentHashMap(4);
        
        int count = 10000;
        for (int i = 0; i < count; i++) {
            map.put("Key" + i, "Value"+ i);
        }
        Assert.assertEquals(map.size(), count);
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.get("Key" + i), "Value"+ i);
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.putIfAbsent("Key" + i, "Value"+ (i * 2)), "Value"+ i);
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.replace("Key" + i, "Value"+ (i * 2)), "Value"+ i);
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.get("Key" + i), "Value"+ (i * 2));
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.put("Key" + i, "Value"+ i), "Value"+ (i * 2));
        }
        
        Assert.assertEquals(map.keySet().size(), count);
        Assert.assertEquals(map.values().size(), count);
        Assert.assertEquals(map.entrySet().size(), count);
        Assert.assertEquals(map.size(), count);
        map.clear();
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
        Assert.assertTrue(map.keySet().isEmpty());
        Assert.assertTrue(map.values().isEmpty());
        Assert.assertTrue(map.entrySet().isEmpty());
        Assert.assertEquals(map.size(), 0);
        
        for (int i = 0; i < count; i++) {
            map.put("Key" + i, "Value"+ i);
        }
        Assert.assertEquals(map.size(), count);
        
        for (int i = 0; i < count; i++) {
            map.remove("Key" + i);
        }
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
        Assert.assertTrue(map.keySet().isEmpty());
        Assert.assertTrue(map.values().isEmpty());
        Assert.assertTrue(map.entrySet().isEmpty());
        Assert.assertEquals(map.size(), 0);
    }
}
