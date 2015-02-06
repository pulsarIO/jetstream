/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.map;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;

/**
 * OffHeap hash map.
 * 
 * @author xingwang
 * 
 * @param <K>
 * @param <V>
 */
public class OffHeapHashMap<K, V> extends OffHeapMapUtilClass<K,V> implements Map<K, V> {

    public OffHeapHashMap(MapDirectMemoryManager memeoryManager, int hashCapacity,
            OffHeapSerializer<K> keySerializer, OffHeapSerializer<V> valueSerializer) {
    	super(memeoryManager, hashCapacity, keySerializer, valueSerializer);

    }

    @Override
    public boolean containsKey(Object o) {
        @SuppressWarnings("unchecked")
        K k = (K) o;
        int index = k.hashCode() & mask;
        long root = hashEntries[index];
        long e = root;
        ByteBuffer keyBuffer = keySerializer.serialize(k);
        while (e != MapDirectMemoryManager.NULL_ADDRESS) {
            if (memoryManager.isKey(e, keyBuffer)) {
                return true;
            } else {
                e = memoryManager.getNext(e);
            }
        }
        return false;
    }

    @Override
    public int size() {
        return usedCount;
    }

    public V putIfAbsent(K k, V v) {
        int index = k.hashCode() & mask;
        long root = hashEntries[index];
        long e = root;
        ByteBuffer keyBuffer = keySerializer.serialize(k);
        while (e != MapDirectMemoryManager.NULL_ADDRESS) {
            if (memoryManager.isKey(e, keyBuffer)) {
                return deserializeValue(memoryManager.getValue(e));
            } else {
                e = memoryManager.getNext(e);
            }
        }

        keyBuffer = memoryManager.copyKeyBuffer(keyBuffer);
        ByteBuffer value = valueSerializer.serialize(v);
        long newEntry = memoryManager.setKeyValue(keyBuffer, value);
        if (newEntry == MapDirectMemoryManager.NULL_ADDRESS) {
            throw new IllegalStateException("No enough off heap memory");
        }
        memoryManager.setNext(newEntry, hashEntries[index]);
        usedCount++;

        hashEntries[index] = newEntry;
        return null;
    }

    @Override
    public V put(K k, V v) {
        int index = k.hashCode() & mask;
        long root = hashEntries[index];
        long e = root;
        long p = root;
        ByteBuffer keyBuffer = keySerializer.serialize(k);

        while (e != MapDirectMemoryManager.NULL_ADDRESS) {
            if (memoryManager.isKey(e, keyBuffer)) {
                V old = deserializeValue(memoryManager.getValue(e));
                keyBuffer = memoryManager.copyKeyBuffer(keyBuffer);
                ByteBuffer value = valueSerializer.serialize(v);
                long newAddress = memoryManager.setKeyValue(keyBuffer, value);
                if (newAddress == MapDirectMemoryManager.NULL_ADDRESS) {
                    throw new IllegalStateException("No enough off heap memory");
                }
                memoryManager.setNext(newAddress, memoryManager.getNext(e));
                memoryManager.free(e);

                if (e == root) {
                    hashEntries[index] = newAddress;
                } else {
                    memoryManager.setNext(p, newAddress);
                }
                return old;
            } else {
                p = e;
                e = memoryManager.getNext(e);
            }
        }

        keyBuffer = memoryManager.copyKeyBuffer(keyBuffer);
        ByteBuffer value = valueSerializer.serialize(v);
        long newEntry = memoryManager.setKeyValue(keyBuffer, value);
        if (newEntry == MapDirectMemoryManager.NULL_ADDRESS) {
            throw new IllegalStateException("No enough off heap memory");
        }
        memoryManager.setNext(newEntry, hashEntries[index]);
        usedCount++;

        hashEntries[index] = newEntry;
        return null;
    }

    @Override
    public V remove(Object o) {
        @SuppressWarnings("unchecked")
        K k = (K) o;
        int index = k.hashCode() & mask;
        long root = hashEntries[index];
        long e = root;
        long p = root;
        ByteBuffer keyBuffer = keySerializer.serialize(k);
        while (e != MapDirectMemoryManager.NULL_ADDRESS) {
            if (memoryManager.isKey(e, keyBuffer)) {
                V v = deserializeValue(memoryManager.getValue(e));
                if (root == e) {
                    hashEntries[index] = memoryManager.getNext(e);
                } else {
                    memoryManager.setNext(p, memoryManager.getNext(e));
                }
                memoryManager.free(e);
                usedCount--;
                return v;
            } else {
                p = e;
                e = memoryManager.getNext(e);
            }
        }
        return null;
    }

    @Override
    public void clear() {
        for (int i = 0; i < hashEntries.length; i++) {
            long e = hashEntries[i];
            while (e != MapDirectMemoryManager.NULL_ADDRESS) {
                long n = memoryManager.getNext(e);
                memoryManager.free(e);
                usedCount--;
                e = n;
            }
            hashEntries[i] = MapDirectMemoryManager.NULL_ADDRESS;
        }

    }

    @Override
    public Set<K> keySet() {
        Set<K> keySet = new HashSet<K>();
        for (int i = 0; i < hashEntries.length; i++) {
            long e = hashEntries[i];
            while (e != MapDirectMemoryManager.NULL_ADDRESS) {
                long n = memoryManager.getNext(e);
                keySet.add(deserializeKey(memoryManager.getKey(e)));
                e = n;
            }
        }
        return keySet;
    }

    @Override
    public boolean containsValue(Object value) {
        for (int i = 0; i < hashEntries.length; i++) {
            long e = hashEntries[i];
            while (e != MapDirectMemoryManager.NULL_ADDRESS) {
                long n = memoryManager.getNext(e);
                V v = deserializeValue(memoryManager.getValue(e));
                if (v.equals(value)) {
                    return true;
                }
                e = n;
            }
        }
        return false;
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        Set<java.util.Map.Entry<K, V>> entrySet = new HashSet<java.util.Map.Entry<K, V>>();
        for (int i = 0; i < hashEntries.length; i++) {
            long e = hashEntries[i];
            while (e != MapDirectMemoryManager.NULL_ADDRESS) {
                long n = memoryManager.getNext(e);
                OffHeapMapEntry<K, V> entry = new OffHeapMapEntry<K, V>(deserializeKey(memoryManager.getKey(e)),
                        deserializeValue(memoryManager.getValue(e)));
                entrySet.add(entry);
                e = n;
            }
        }
        return entrySet;
    }

    @Override
    public boolean isEmpty() {
        return usedCount == 0;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            K key = e.getKey();
            V value = e.getValue();
            put(key, value);
        }

    }

    @Override
    public Collection<V> values() {
        List<V> values = new LinkedList<V>();
        for (int i = 0; i < hashEntries.length; i++) {
            long e = hashEntries[i];
            while (e != MapDirectMemoryManager.NULL_ADDRESS) {
                long n = memoryManager.getNext(e);
                values.add(deserializeValue(memoryManager.getValue(e)));
                e = n;
            }
        }
        return values;
    }
    
    protected void finalize() {
        clear();
    }
}
