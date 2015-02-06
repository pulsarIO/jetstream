/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.ebay.jetstream.util.offheap.map.MapDirectMemoryManagerImpl;

public class MapMemoryManagerTest {
    @Test
    public void testGC() {
        int entrySize = 128;
        int pageSize = 8192;
        int pageNum = 40;
        //64M;
        
        for (int x = 0; x < 10; x ++) {
            MapDirectMemoryManagerImpl cache = new MapDirectMemoryManagerImpl(pageSize, pageNum, entrySize);
            String key = "";
            String value = "";
            
            for (int i = 0;  i < Integer.MAX_VALUE; i++) {
                if (i %2 == 0) {
                    key = key + (i % 10);
                } else {
                    value = value + (i % 10);
                }
                long kvAddress = cache.setKeyValue(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(value.getBytes()));
    
                if (kvAddress == -1) {
                    break;
                }
                Assert.assertEquals(cache.getKeyLength(kvAddress), key.length());
                Assert.assertEquals(cache.getValueLength(kvAddress), value.length());
                ByteBuffer keyBuffer = ByteBuffer.allocate(key.length());
                cache.readKey(kvAddress, keyBuffer);
                Assert.assertEquals(new String(keyBuffer.array()), key);
                ByteBuffer valueBuffer = ByteBuffer.allocate( value.length());
                cache.readValue(kvAddress, valueBuffer);
                Assert.assertEquals(new String(valueBuffer.array()), value);
                
            }
            
        }
    }
    
    @Test
    public void testMalloc() {
        int entrySize = 128;
        int pageSize = 1024;
        int pageNum = 5;
        int entryPerPage = pageSize / entrySize;
        MapDirectMemoryManagerImpl cache = new MapDirectMemoryManagerImpl(pageSize, pageNum, entrySize);

        Assert.assertEquals(cache.getUsedMemory(), 0);
        String key = "";
        String value = "";
        
        for (int i = 0;  i < Integer.MAX_VALUE; i++) {
            key = key + (i % 10);
            long kvAddress = cache.setKeyValue(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(value.getBytes()));
            if (kvAddress == -1) {
                Assert.assertEquals(entryPerPage * pageNum * (entrySize - MapDirectMemoryManagerImpl.HEADER_LENGTH) - 4, key.length() + value.length() + 8 - 1 + 32);
                break;
            }
            Assert.assertEquals(cache.getKeyLength(kvAddress), key.length());
            Assert.assertEquals(cache.getValueLength(kvAddress), value.length());
            ByteBuffer keyBuffer = ByteBuffer.allocate(key.length());
            cache.readKey(kvAddress, keyBuffer);
            Assert.assertEquals(new String(keyBuffer.array()), key);
            ByteBuffer valueBuffer = ByteBuffer.allocate( value.length());
            cache.readValue(kvAddress, valueBuffer);
            Assert.assertEquals(new String(valueBuffer.array()), value);
            cache.free(kvAddress);
        }
        
        key = "";
        value = "";
        
        for (int i = 0;  i < Integer.MAX_VALUE; i++) {
            value = value + (i % 10);
            long kvAddress = cache.setKeyValue(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(value.getBytes()));
            if (kvAddress == -1) {
                Assert.assertEquals(entryPerPage * pageNum * (entrySize - MapDirectMemoryManagerImpl.HEADER_LENGTH) - 4, key.length() + value.length() + 8 - 1 + 32);
                break;
            }
            Assert.assertEquals(cache.getKeyLength(kvAddress), key.length());
            Assert.assertEquals(cache.getValueLength(kvAddress), value.length());
            ByteBuffer keyBuffer = ByteBuffer.allocate(key.length());
            cache.readKey(kvAddress, keyBuffer);
            Assert.assertEquals(new String(keyBuffer.array()), key);
            ByteBuffer valueBuffer = ByteBuffer.allocate( value.length());
            cache.readValue(kvAddress, valueBuffer);
            Assert.assertEquals(new String(valueBuffer.array()), value);
            cache.free(kvAddress);
        }
        
        key = "";
        value = "";
        
        for (int i = 0;  i < Integer.MAX_VALUE; i++) {
            if (i %2 == 0) {
                key = key + (i % 10);
            } else {
                value = value + (i % 10);
            }
            long kvAddress = cache.setKeyValue(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(value.getBytes()));
            if (kvAddress == -1) {
                Assert.assertEquals(entryPerPage * pageNum * (entrySize - MapDirectMemoryManagerImpl.HEADER_LENGTH) - 4, key.length() + value.length() + 8 - 1 + 32);
                break;
            }
            Assert.assertEquals(cache.getKeyLength(kvAddress), key.length());
            Assert.assertEquals(cache.getValueLength(kvAddress), value.length());
            ByteBuffer keyBuffer = ByteBuffer.allocate(key.length());
            cache.readKey(kvAddress, keyBuffer);
            Assert.assertEquals(new String(keyBuffer.array()), key);
            ByteBuffer valueBuffer = ByteBuffer.allocate( value.length());
            cache.readValue(kvAddress, valueBuffer);
            Assert.assertEquals(new String(valueBuffer.array()), value);
            cache.free(kvAddress);
        }
    }
}
