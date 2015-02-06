/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.map;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;

public class OffHeapMapUtilClass<K, V> {
    protected static final int DEFAULT_CAPACITY = 1 << 10;
    protected final long[] hashEntries;
    protected long head = MapDirectMemoryManager.NULL_ADDRESS;
    protected final OffHeapSerializer<K> keySerializer;
    protected final int mask;
    protected final MapDirectMemoryManager memoryManager;
    protected long tail = MapDirectMemoryManager.NULL_ADDRESS;
    protected int usedCount;
    protected final OffHeapSerializer<V> valueSerializer;

    
    public OffHeapMapUtilClass(MapDirectMemoryManager memeoryManager, int hashCapacity,
            OffHeapSerializer<K> keySerializer, OffHeapSerializer<V> valueSerializer) {
        this.memoryManager = memeoryManager;
        if (keySerializer != null) {
            this.keySerializer = keySerializer;
        } else {
            this.keySerializer = DefaultSerializerFactory.getInstance().createObjectSerializer();
        }
        if (valueSerializer != null) {
            this.valueSerializer = valueSerializer;
        } else {
            this.valueSerializer = DefaultSerializerFactory.getInstance().createObjectSerializer();
        }
        int normalizedCapacity = hashCapacity;
        if (normalizedCapacity <= 0) {
            normalizedCapacity = DEFAULT_CAPACITY;
        }
        int x = 2;
        while (x < normalizedCapacity) {
            x = x << 1;
        }
        normalizedCapacity = x;
        mask = normalizedCapacity - 1;
        hashEntries = new long[normalizedCapacity];
        Arrays.fill(hashEntries, MapDirectMemoryManager.NULL_ADDRESS);
    }
    
    protected K deserializeKey(ByteBuffer buf) {
        return keySerializer.deserialize(buf, buf.position(), buf.limit() - buf.position());
    }

    protected V deserializeValue(ByteBuffer buf) {
        return valueSerializer.deserialize(buf, buf.position(), buf.limit() - buf.position());
    }
    

    public V get(Object o) {
        @SuppressWarnings("unchecked")
        K k = (K) o;
        int index = k.hashCode() & mask;
        long root = hashEntries[index];
        long e = root;
        ByteBuffer keyBuffer = keySerializer.serialize(k);
        while (e != MapDirectMemoryManager.NULL_ADDRESS) {
            if (memoryManager.isKey(e, keyBuffer)) {
                V v = deserializeValue(memoryManager.getValue(e));
                return v;
            } else {
                e = memoryManager.getNext(e);
            }
        }
        return null;
    }
}
