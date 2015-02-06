/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.map;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.ebay.jetstream.util.offheap.OffHeapCache;
import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;

/**
 * Offheap linked hash map with expiration time support.
 * 
 * @author xingwang
 * 
 * @param <K>
 * @param <V>
 */
public class OffHeapMapCacheImpl<K, V> implements OffHeapCache<K,V> {
    private static final int DEFAULT_CAPACITY = 1 << 10;
    private long expiredTimestamp;
    private final long[] hashEntries;
    private final long head[];
    private final OffHeapSerializer<K> keySerializer;
    private final int mask;
    private long maxTimestamp;
    private final MapDirectMemoryManager memoryManager;
    private final long tail[];
    private final int timeSlots;
    private final long timestamp[];
    private int usedCount;
    private final OffHeapSerializer<V> valueSerializer;

    public OffHeapMapCacheImpl(MapDirectMemoryManager memeoryManager, int hashCapacity, OffHeapSerializer<K> keySerializer,
            OffHeapSerializer<V> valueSerializer, int timeSlots) {
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
        this.memoryManager = memeoryManager;
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
        this.timeSlots = timeSlots;
        head = new long[timeSlots];
        tail = new long[timeSlots];
        timestamp = new long[timeSlots];
        Arrays.fill(head, MapDirectMemoryManager.NULL_ADDRESS);
        Arrays.fill(tail, MapDirectMemoryManager.NULL_ADDRESS);
        Arrays.fill(timestamp, MapDirectMemoryManager.NULL_ADDRESS);
        expiredTimestamp = convertToSecond(System.currentTimeMillis());
        maxTimestamp = expiredTimestamp + timeSlots;
    }

    private void addToLinkedList(K k, V v, long newEntry, int slot, long slotTimestamp) {

        if (timestamp[slot] == MapDirectMemoryManager.NULL_ADDRESS) {
            timestamp[slot] = slotTimestamp;
        }
        if (head[slot] == MapDirectMemoryManager.NULL_ADDRESS) {
            head[slot] = tail[slot] = newEntry;
        } else {
            memoryManager.setRight(tail[slot], newEntry);
            memoryManager.setLeft(newEntry, tail[slot]);
            tail[slot] = newEntry;
        }
    }

    private long convertToSecond(long timestamp) {
        return timestamp / 1000;
    }

    private K deserializeKey(ByteBuffer buf) {
        return keySerializer.deserialize(buf, buf.position(), buf.limit() - buf.position());
    }

    private V deserializeValue(ByteBuffer buf) {
        return valueSerializer.deserialize(buf, buf.position(), buf.limit() - buf.position());
    }

    @Override
    public V get(K k) {
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

    @Override
    public Map.Entry<K, V> getFirst() {
        int firstSlot = (int) expiredTimestamp % timeSlots;
        int c = 0;
        while (c < timeSlots) {
            if (head[firstSlot] != MapDirectMemoryManager.NULL_ADDRESS) {
                V v = deserializeValue(memoryManager.getValue(head[firstSlot]));
                K k = deserializeKey(memoryManager.getKey(head[firstSlot]));
                return new OffHeapMapEntry<K, V>(k, v);
            } else {
                firstSlot++;
                if (firstSlot == timeSlots) {
                    firstSlot -= timeSlots;
                }
                c++;
            }
        }
        return null;
    }

    @Override
    public int size() {
        return usedCount;
    }

    @Override
    public boolean put(K k, V v, long expirationTime) {
        int index = k.hashCode() & mask;
        long root = hashEntries[index];
        long e = root;
        long p = root;
        long slotTimestamp = convertToSecond(expirationTime);
        if (slotTimestamp < expiredTimestamp) {
            slotTimestamp = expiredTimestamp;
            expirationTime = slotTimestamp * 1000;
        } else if (slotTimestamp > maxTimestamp) {
            slotTimestamp = maxTimestamp;
            expirationTime = slotTimestamp * 1000;
        }
        int slot = (int) (slotTimestamp % timeSlots);
        ByteBuffer keyBuffer = keySerializer.serialize(k);
        while (e != MapDirectMemoryManager.NULL_ADDRESS) {
            if (memoryManager.isKey(e, keyBuffer)) {
                keyBuffer = memoryManager.copyKeyBuffer(keyBuffer);
                ByteBuffer value = valueSerializer.serialize(v);
                long newAddress = memoryManager.setKeyValue(keyBuffer, value);
                if (newAddress == MapDirectMemoryManager.NULL_ADDRESS) {
                    return false;
                }
                memoryManager.setTimestamp(newAddress, expirationTime);
                memoryManager.setNext(newAddress, memoryManager.getNext(e));
                removeFromLinkedList(e);
                memoryManager.free(e);
                addToLinkedList(k, v, newAddress, slot, slotTimestamp);

                if (e == root) {
                    hashEntries[index] = newAddress;
                } else {
                    memoryManager.setNext(p, newAddress);
                }
                return true;
            } else {
                p = e;
                e = memoryManager.getNext(e);
            }
        }

        keyBuffer = memoryManager.copyKeyBuffer(keyBuffer);
        ByteBuffer value = valueSerializer.serialize(v);
        long newEntry = memoryManager.setKeyValue(keyBuffer, value);
        if (newEntry == MapDirectMemoryManager.NULL_ADDRESS) {
            return false;
        }
        memoryManager.setTimestamp(newEntry, expirationTime);
        memoryManager.setNext(newEntry, hashEntries[index]);
        usedCount++;

        hashEntries[index] = newEntry;
        addToLinkedList(k, v, newEntry, slot, slotTimestamp);
        return true;
    }

    @Override
    public boolean remove(K k) {
        int index = k.hashCode() & mask;
        long root = hashEntries[index];
        long e = root;
        long p = root;
        ByteBuffer keyBuffer = keySerializer.serialize(k);
        while (e != MapDirectMemoryManager.NULL_ADDRESS) {
            if (memoryManager.isKey(e, keyBuffer)) {
                removeFromLinkedList(e);
                if (root == e) {
                    hashEntries[index] = memoryManager.getNext(e);
                } else {
                    memoryManager.setNext(p, memoryManager.getNext(e));
                }
                memoryManager.free(e);
                usedCount--;
                return true;
            } else {
                p = e;
                e = memoryManager.getNext(e);
            }
        }
        return false;
    }

    private void removeEntry(K k, long e) {
        int index = k.hashCode() & mask;
        long root = hashEntries[index];

        if (root == e) {
            hashEntries[index] = memoryManager.getNext(e);
        } else {
            long p = root;
            long x = root;
            do {
                p = x;
                x = memoryManager.getNext(p);
            } while (x != e);

            memoryManager.setNext(p, memoryManager.getNext(e));
        }
        removeFromLinkedList(e);
        memoryManager.free(e);
        usedCount--;
    }

    @Override
    public Map.Entry<K, V> removeExpiredData(long expirationTime) {
        long newExpiredTimestamp = convertToSecond(expirationTime);
        if (newExpiredTimestamp < expiredTimestamp) {
            return null;
        }

        long checkedTimestamp = expiredTimestamp;
        int idx = (int) (expiredTimestamp % timeSlots);
        int c = 0;
        while (checkedTimestamp < newExpiredTimestamp && c < timeSlots) {
            if (head[idx] != MapDirectMemoryManager.NULL_ADDRESS) {
                V v = deserializeValue(memoryManager.getValue(head[idx]));
                K k = deserializeKey(memoryManager.getKey(head[idx]));

                removeEntry(k, head[idx]);
                return new OffHeapMapEntry<K, V>(k, v);
            }
            checkedTimestamp += 1;
            this.expiredTimestamp = checkedTimestamp;
            this.maxTimestamp = expiredTimestamp + timeSlots;
            c++;
            idx = (int) (expiredTimestamp % timeSlots);
        }
        return null;
    }

    @Override
    public Map.Entry<K, V> removeFirst() {
        int firstSlot = (int) expiredTimestamp % timeSlots;
        int c = 0;
        while (c < timeSlots) {
            if (head[firstSlot] != MapDirectMemoryManager.NULL_ADDRESS) {
                V v = deserializeValue(memoryManager.getValue(head[firstSlot]));
                K k = deserializeKey(memoryManager.getKey(head[firstSlot]));
                removeEntry(k, head[firstSlot]);

                return new OffHeapMapEntry<K, V>(k, v);
            } else {
                firstSlot++;
                if (firstSlot == timeSlots) {
                    firstSlot -= timeSlots;
                }
                c++;
            }
        }
        return null;
    }

    private void removeFromLinkedList(long e) {
        long oldExpiredTime = memoryManager.getTimestamp(e);
        int slot = (int) (convertToSecond(oldExpiredTime) % timeSlots);
        long rightAddress = memoryManager.getRight(e);
        long leftAddress = memoryManager.getLeft(e);
        if (e == head[slot]) {
            head[slot] = rightAddress;
            if (head[slot] == MapDirectMemoryManager.NULL_ADDRESS) {
                timestamp[slot] = MapDirectMemoryManager.NULL_ADDRESS;
            }
        } else {
            memoryManager.setRight(leftAddress, rightAddress);
        }
        if (e == tail[slot]) {
            tail[slot] = leftAddress;
        } else {
            memoryManager.setLeft(rightAddress, leftAddress);
        }
        memoryManager.setLeft(e, MapDirectMemoryManager.NULL_ADDRESS);
        memoryManager.setRight(e, MapDirectMemoryManager.NULL_ADDRESS);
    }

    protected void finalize() {
        if (usedCount > 0) {
            clear();
        }
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
        Arrays.fill(head, MapDirectMemoryManager.NULL_ADDRESS);
        Arrays.fill(tail, MapDirectMemoryManager.NULL_ADDRESS);
        Arrays.fill(timestamp, MapDirectMemoryManager.NULL_ADDRESS);
        expiredTimestamp = convertToSecond(System.currentTimeMillis());
        maxTimestamp = expiredTimestamp + timeSlots;
        
    }
}
