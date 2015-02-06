/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.ebay.jetstream.util.offheap.map.MapDirectMemoryManagerImpl;
import com.ebay.jetstream.util.offheap.map.OffHeapConcurrentHashMap;
import com.ebay.jetstream.util.offheap.map.OffHeapHashMap;
import com.ebay.jetstream.util.offheap.map.OffHeapLinkedHashMapImpl;
import com.ebay.jetstream.util.offheap.map.OffHeapMapCacheImpl;

/**
 * The builder for off heap maps.
 * 
 * The build method can be invoked multiple times and all the maps
 * created (included maps build from cloned builder) will share
 * same native memory.
 * 
 * @author xingwang
 *
 * @param <K>
 * @param <V>
 */
public class MapBuilder<K, V> {
    public static enum Unit {
        GB(1 << 30), KB(1 << 10), MB(1 << 20), B(1);

        private long v;

        private Unit(long l) {
            this.v = l;
        }
    }
    
    public static <K, V> MapBuilder<K, V> newBuilder() {
        return new MapBuilder<K, V>();
    }
    
    private int hashCapacity = 1 << 20;
    private long maxMemory = 128;
    private int pageSize = 8192;
    private int blockSize = 128;
    private Unit unit = Unit.MB;

    private MapDirectMemoryManagerImpl memoryManager;
    private OffHeapSerializer<K> keySerializer;
    private OffHeapSerializer<V> valueSerializer;
    
    /**
     * Build an off heap hash map.
     * 
     * @return
     */
    public synchronized Map<K, V> buildHashMap() {
        checkMemoryManager();
        return new OffHeapHashMap<K, V>(memoryManager, hashCapacity, keySerializer, valueSerializer);
    }
    
    /**
     * Build an off heap concurrent hash map.
     * 
     * @param concurencyLevel
     * @return
     */
    public synchronized ConcurrentMap<K, V> buildConcurrentHashMap(int concurencyLevel) {
        checkMemoryManager();
        return new OffHeapConcurrentHashMap<K, V>(memoryManager, hashCapacity, keySerializer, valueSerializer, concurencyLevel);
    }
    
    /**
     * Build an off heap linked hash map.
     * 
     * @return
     */
    public synchronized OffHeapLinkedHashMap<K,V> buildLinkedHashMap() {
        checkMemoryManager();
        return new OffHeapLinkedHashMapImpl<K, V>(memoryManager, hashCapacity, keySerializer, valueSerializer);
    }
    
    /**
     * Build an off heap cache with expiration supprot.
     * 
     * @return
     */
    public synchronized OffHeapCache<K,V> buildCache(int expirationTimeSlots) {
        checkMemoryManager();
        return new OffHeapMapCacheImpl<K, V>(memoryManager, hashCapacity, keySerializer, valueSerializer, expirationTimeSlots);
    }
    
    /**
     * Specify the hash capacity.
     * 
     * @param capacity
     * @return
     */
    public MapBuilder<K, V> withHashCapacity(int capacity) {
        this.hashCapacity = capacity;
        return this;
    }
    
    /**
     * Limit the max memory used by the queue, by default it is 128.
     * 
     * @param maxMemory
     * @return
     */
    public MapBuilder<K, V> withMaxMemory(long maxMemory) {
        if (memoryManager != null) {
            throw new IllegalStateException("Can not change max memory after the memory manager initalized");
        }
        this.maxMemory = maxMemory;
        return this;
    }

    /**
     * Configure the page size. By default it is 8192.
     * 
     * The page size should be greater than the item size in bytes. And better
     * to allow one page can include at least 10 items.
     * 
     * @param pageSize
     * @return
     */
    public MapBuilder<K, V> withPageSize(int pageSize) {
        if (memoryManager != null) {
            throw new IllegalStateException("Can not change pageSize after the memory manager initalized");
        }
        this.pageSize = pageSize;
        return this;
    }

    /**
     * An key serializer to optimize the serialization. Must specified.
     * 
     * @param serializer
     * @return
     */
    public MapBuilder<K, V> withKeySerializer(OffHeapSerializer<K> serializer) {
        this.keySerializer = serializer;
        return this;
    }

    /**
     * An value serializer to optimize the serialization. Must specified.
     * 
     * @param serializer
     * @return
     */
    public MapBuilder<K, V> withValueSerializer(OffHeapSerializer<V> serializer) {
        this.valueSerializer = serializer;
        return this;
    }
    
    /**
     * Define the unit for the native memory. By default it is MB.
     * 
     * @param unit
     * @return
     */
    public MapBuilder<K, V> withUnit(Unit unit) {
        if (memoryManager != null) {
            throw new IllegalStateException("Can not change unit after the memory manager initalized");
        }
        this.unit = unit;
        return this;
    }
    
    public MapBuilder<K, V> withBlockSize(int blockSize) {
        if (memoryManager != null) {
            throw new IllegalStateException("Can not change block size after the memory manager initalized");
        }
        this.blockSize = blockSize;
        return this;
    }
    
    /**
     * Clone another builder instance which shared same memory manager.
     * 
     * This provide a mechanism for multiple different type maps shared
     * a big memory. 
     * 
     * @return
     */
    public synchronized <K1, V1> MapBuilder<K1, V1> cloneBuilder() {
        MapBuilder<K1, V1> b = new MapBuilder<K1, V1>();
        checkMemoryManager();
        b.memoryManager = this.memoryManager;
        return b;
    }
    
    /**
     * Return the memory manager created or cloned by this builder.
     * 
     * @return
     */
    public OffHeapMemoryManager getOffHeapMemoryManager() {
        checkMemoryManager();
        return memoryManager;
    }
    
    private void checkMemoryManager() {
        if (memoryManager == null) {
            long totalMemoryInBytes = maxMemory * unit.v;
            int pageNum = (int) (totalMemoryInBytes / pageSize);
            memoryManager = new MapDirectMemoryManagerImpl(pageSize, pageNum, blockSize);
        }
    }
}
