/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;

public class OffHeapLinkedMapTest {
    @Test
    public void testMap() {
        OffHeapSerializer<String> strSerializer = DefaultSerializerFactory.getInstance().getStringSerializer();
        MapBuilder<String, String> builder = MapBuilder.newBuilder();
        OffHeapLinkedHashMap<String, String> map = builder.withKeySerializer(strSerializer).withValueSerializer(strSerializer).buildLinkedHashMap();
        
        int count = 10000;
        for (int i = 0; i < count; i++) {
            map.put("Key" + i, "Value"+ i);
        }
        Assert.assertEquals(map.size(), count);
        
        {
            Iterator<String> keyIterator = map.keyIterator();
            int i = 0;
            while (keyIterator.hasNext()) {
                Assert.assertEquals("Key" + i, keyIterator.next());
                i ++;
            }
            Assert.assertEquals(map.size(), i);
        }
        
        {
            Iterator<Entry<String, String>> entryIterator = map.entryIterator();
            int i = 0;
            while (entryIterator.hasNext()) {
                Entry<String, String> entry = entryIterator.next();
                Assert.assertEquals("Key" + i, entry.getKey());
                Assert.assertEquals("Value" + i, entry.getValue());
                i ++;
            }
            Assert.assertEquals(map.size(), i);
        }
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.removeFirst().getValue(), "Value"+ i);
        }
        
        Assert.assertEquals(map.size(), 0);
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);

        for (int i = 0; i < count; i++) {
            map.put("Key" + i, "Value"+ i);
        }
        
        Assert.assertEquals(map.size(), count);
        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(map.get("Key" + i), "Value"+ i);
        }
        
        for (int i = count - 1; i >= 0; i--) {
            map.put("Key" + i, "Value"+ (i * 2));
        }
        
        for (int i = count - 1; i >= 0; i--) {
            Assert.assertEquals(map.removeFirst().getValue(), "Value"+ (i * 2));
        }
        
        Assert.assertEquals(map.size(), 0);
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
        for (int i = count - 1; i >= 0; i--) {
            map.put("Key" + i, "Value"+ (i * 2));
        }
        
        Assert.assertEquals(map.size(), count);
        
        map.clear();
        
        Assert.assertEquals(map.size(), 0);
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
        for (int i = count - 1; i >= 0; i--) {
            map.put("Key" + i, "Value"+ (i * 2));
        }
        
        Assert.assertEquals(map.size(), count);
        
        map.clear();
        
        Assert.assertEquals(map.size(), 0);
        
        Assert.assertEquals(builder.getOffHeapMemoryManager().getUsedMemory(), 0);
        
    }
    
}
