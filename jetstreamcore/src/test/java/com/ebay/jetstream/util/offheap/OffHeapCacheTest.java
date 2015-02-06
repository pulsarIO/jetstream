/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;

public class OffHeapCacheTest {
    @Test
    public void testMap3() {
        OffHeapSerializer<String> strSerializer = DefaultSerializerFactory.getInstance().getStringSerializer();
        MapBuilder<String, String> builder = MapBuilder.newBuilder();
        OffHeapCache<String, String> cache = builder.withKeySerializer(strSerializer).withValueSerializer(strSerializer).withHashCapacity(8).buildCache(1200);
        
        long currentTime = System.currentTimeMillis();
        int count = 2000;
        for (int i = 0; i < count; i++) {
            cache.put("Key" + i, "" + (currentTime + i), currentTime + i);
        }
        Assert.assertEquals(cache.size(), count);
        
        for (int i = 0; i < count; i++) {
            Entry<String, String> v = cache.removeExpiredData(currentTime + 20000 );
            //System.out.println(v.getKey() + ":" + v.getValue());
            cache.put(v.getKey(), v.getValue(), currentTime + i);
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(cache.get("Key" + i), "" + (currentTime + i));
        }
        
        Assert.assertEquals(cache.size(), count); 
        
        java.util.Set<String> s = new java.util.HashSet<String>();
        for (int i = 0; i < count; i++) {
            Entry<String, String> v = cache.removeExpiredData(currentTime + 20000 );
            if (s.contains(v.getKey())) {
                System.out.println("Duplicate:" + s);
            } else {
                s.add(v.getKey());
            }
        }
        Assert.assertEquals(cache.size(), 0);
    }
    
    
    
    @Test
    public void testMap2() {
        OffHeapSerializer<String> strSerializer = DefaultSerializerFactory.getInstance().getStringSerializer();
        MapBuilder<String, String> builder = MapBuilder.newBuilder();
        OffHeapCache<String, String> cache = builder.withKeySerializer(strSerializer).withValueSerializer(strSerializer).withHashCapacity(8).buildCache(1200);
        
        long currentTime = System.currentTimeMillis();
        int count = 10000;
        for (int i = 0; i < count; i++) {
            cache.put("Key" + i, "" + (currentTime + i), currentTime + i);
        }
        Assert.assertEquals(cache.size(), count);
        
        for (int i = 0; i < count; i++) {
            cache.remove("Key" + i);
        }
        
        Assert.assertEquals(cache.size(), 0);
        
        System.out.println(cache.removeExpiredData(currentTime + 1000000));
    }
    
    
    @Test
    public void testMap() {
        OffHeapSerializer<String> strSerializer = DefaultSerializerFactory.getInstance().getStringSerializer();
        MapBuilder<String, String> builder = MapBuilder.newBuilder();
        OffHeapCache<String, String> cache = builder.withKeySerializer(strSerializer).withValueSerializer(strSerializer).withHashCapacity(8192).buildCache(1200);
        
        long currentTime = System.currentTimeMillis();
        int count = 10000;
        for (int i = 0; i < count; i++) {
            cache.put("Key" + i, "" + (currentTime + i), currentTime + i);
        }
        Assert.assertEquals(cache.size(), count);
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(cache.get("Key" + i), "" + (currentTime + i));
        }
        
        long timestamp = (currentTime / 1000) * 1000;
        for (int i = 0; i < count; i++) {
            Entry<String, String> entry = cache.removeFirst();
            
            long ts = (Long.valueOf(entry.getValue()) / 1000) * 1000;
            Assert.assertTrue(timestamp <= ts);
            timestamp = ts;
        }
        
        
        Assert.assertEquals(cache.size(), 0);
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
        for (int i = 0; i < count; i++) {
            cache.put("Key" + i, "" + (currentTime + ( i % 20 ) * 1000), currentTime + ( i % 20 ) * 1000);
        }
        Assert.assertEquals(cache.size(), count);
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(cache.get("Key" + i), "" + (currentTime + ( i % 20 ) * 1000));
        }
        
        
        timestamp = (currentTime / 1000) * 1000;
        for (int i = 0; i < count; i++) {
            Entry<String, String> entry = cache.removeExpiredData(currentTime + 1000000);
            
            long ts = (Long.valueOf(entry.getValue()) / 1000) * 1000;
            Assert.assertTrue(timestamp <= ts);
            timestamp = ts;
        }
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        for (int i = 0; i < count; i++) {
            Entry<String, String> entry = cache.removeExpiredData(currentTime + 1000000);
            
            Assert.assertTrue(entry == null);
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertNull(cache.get("Key" + i));
        }
        
        currentTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            cache.put("Key" + i, "" + (currentTime + ( i % 20 ) * 1000), currentTime + ( i % 20 ) * 1000);
        }
        Assert.assertEquals(cache.size(), count);
        
        cache.clear();
        Assert.assertEquals(cache.size(), 0);
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
        currentTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            cache.put("Key" + i, "" + (currentTime + ( i % 20 ) * 1000), currentTime + ( i % 20 ) * 1000);
        }
        Assert.assertEquals(cache.size(), count);
        
        cache.clear();
        Assert.assertEquals(cache.size(), 0);
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);

    }
    
}
