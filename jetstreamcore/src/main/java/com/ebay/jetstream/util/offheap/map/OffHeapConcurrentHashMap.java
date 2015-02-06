/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.map;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;

/**
 * Concurrent Offheap hash map.
 * 
 * @author xingwang
 * 
 * @param <K>
 * @param <V>
 */
public class OffHeapConcurrentHashMap<K, V> implements ConcurrentMap<K, V> {
    private class Segment {
        private final OffHeapHashMap<K, V> localMap;
        private final ReadWriteLock lock;

        public Segment() {
            lock = new ReentrantReadWriteLock();

            localMap = new OffHeapHashMap<K, V>(memeoryManager, hashCapacity, keySerializer, valueSerializer);
        }

        public void clear() {
            lock.writeLock().lock();
            try {
                localMap.clear();
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean containsKey(K key) {
            lock.readLock().lock();
            try {
                return localMap.containsKey(key);
            } finally {
                lock.readLock().unlock();
            }
        }

        public boolean containsValue(Object value) {
            lock.writeLock().lock();
            try {
                return localMap.containsValue(value);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public Set<Map.Entry<K, V>> entrySet() {
            lock.writeLock().lock();
            try {
                return localMap.entrySet();
            } finally {
                lock.writeLock().unlock();
            }
        }

        public V get(K key) {
            lock.readLock().lock();
            try {
                return localMap.get(key);
            } finally {
                lock.readLock().unlock();
            }
        }

        public Set<K> keySet() {
            lock.writeLock().lock();
            try {
                return localMap.keySet();
            } finally {
                lock.writeLock().unlock();
            }
        }

        public V put(K key, V value) {
            lock.writeLock().lock();
            try {
                return localMap.put(key, value);
            } finally {
                lock.writeLock().unlock();
            }

        }

        public V putIfAbsent(K key, V value) {
            lock.writeLock().lock();
            try {
                return localMap.putIfAbsent(key, value);
            } finally {
                lock.writeLock().unlock();
            }

        }

        public V remove(K key) {
            lock.writeLock().lock();
            try {
                return localMap.remove(key);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public int size() {
            lock.readLock().lock();
            try {
                return localMap.size();
            } finally {
                lock.readLock().unlock();
            }
        }

        public Collection<? extends V> values() {
            lock.writeLock().lock();
            try {
                return localMap.values();
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean remove(K key, V value) {
            lock.writeLock().lock();
            try {
                V v = localMap.get(key);
                if (value.equals(v)) {
                    localMap.remove(key);
                    return true;
                }
                return false;
            } finally {
                lock.writeLock().unlock();
            }
        }

        public V replace(K key, V value) {
            lock.writeLock().lock();
            try {
                if (localMap.containsKey(key)) {
                    return localMap.put(key, value);
                }
                return null;
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean replace(K key, V oldValue, V newValue) {
            lock.writeLock().lock();
            try {
                V v = localMap.get(key);
                if (oldValue.equals(v)) {
                    localMap.put(key, newValue);
                    return true;
                }
                return false;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    private final MapDirectMemoryManager memeoryManager;

    private final int hashCapacity;
    private final int segmentMask;
    private final Segment[] segments;

    private final int segmentShift;
    private final OffHeapSerializer<K> keySerializer;
    private final OffHeapSerializer<V> valueSerializer;

    @SuppressWarnings("unchecked")
    public OffHeapConcurrentHashMap(MapDirectMemoryManager memeoryManager, int hashCapacity,
            OffHeapSerializer<K> keySerializer, OffHeapSerializer<V> valueSerializer,
            int concurrencyLevel) {
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
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
        this.hashCapacity = hashCapacity;
        this.memeoryManager = memeoryManager;
        this.segments = (Segment[]) Array.newInstance(Segment.class, ssize);
        for (int i = 0; i < ssize; i++) {
            segments[i] = new Segment();
        }
    }

    @Override
    public void clear() {
        for (int i = 0; i < segments.length; i++) {
            segments[i].clear();
        }
    }

    @Override
    public boolean containsKey(Object k) {
        @SuppressWarnings("unchecked")
        K key = (K) k;
        int hash = hash(key.hashCode());
        return segmentFor(hash).containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (int i = 0; i < segments.length; i++) {
            if (segments[i].containsValue(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        Set<java.util.Map.Entry<K, V>> entrySet = new HashSet<java.util.Map.Entry<K, V>>();
        for (int i = 0; i < segments.length; i++) {
            if (segments[i].size() > 0) {
                entrySet.addAll(segments[i].entrySet());
            }
        }
        return entrySet;
    }

    @Override
    public V get(Object k) {
        @SuppressWarnings("unchecked")
        K key = (K) k;
        int hash = hash(key.hashCode());
        return segmentFor(hash).get(key);
    }

    @Override
    public boolean isEmpty() {
        for (int i = 0; i < segments.length; i++) {
            if (segments[i].size() > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Set<K> keySet() {
        Set<K> keySet = new HashSet<K>();
        for (int i = 0; i < segments.length; i++) {
            if (segments[i].size() > 0) {
                keySet.addAll(segments[i].keySet());
            }
        }
        return keySet;
    }

    @Override
    public V put(K key, V value) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            K key = e.getKey();
            V value = e.getValue();
            int hash = hash(key.hashCode());
            segmentFor(hash).put(key, value);
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).putIfAbsent(key, value);
    }

    @Override
    public V remove(Object k) {
        @SuppressWarnings("unchecked")
        K key = (K) k;
        int hash = hash(key.hashCode());
        return segmentFor(hash).remove(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object key, Object value) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).remove((K) key, (V) value);
    }

    @Override
    public V replace(K key, V value) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).replace(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        int hash = hash(key.hashCode());
        return segmentFor(hash).replace(key, oldValue, newValue);
    }

    final Segment segmentFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    @Override
    public int size() {
        int size = 0;
        for (int i = 0; i < segments.length; i++) {
            size += segments[i].size();
        }
        return size;
    }

    @Override
    public Collection<V> values() {
        Collection<V> valuesCollection = new LinkedList<V>();
        for (int i = 0; i < segments.length; i++) {
            if (segments[i].size() > 0) {
                valuesCollection.addAll(segments[i].values());
            }
        }
        return valuesCollection;
    }
}
