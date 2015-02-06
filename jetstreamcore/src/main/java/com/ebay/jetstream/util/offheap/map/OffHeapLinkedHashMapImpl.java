/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.map;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import com.ebay.jetstream.util.offheap.OffHeapLinkedHashMap;
import com.ebay.jetstream.util.offheap.OffHeapSerializer;

/**
 * OffHeap linked hash map.
 * 
 * @author xingwang
 * 
 * @param <K>
 * @param <V>
 */
public class OffHeapLinkedHashMapImpl<K, V> extends OffHeapMapUtilClass<K,V> implements OffHeapLinkedHashMap<K,V> {

    public OffHeapLinkedHashMapImpl(MapDirectMemoryManager memeoryManager, int hashCapacity,
            OffHeapSerializer<K> keySerializer, OffHeapSerializer<V> valueSerializer) {
    	super(memeoryManager, hashCapacity, keySerializer, valueSerializer);

    }

    private void addToLinkedList(K k, V v, long newEntry) {
        if (head == MapDirectMemoryManager.NULL_ADDRESS) {
            head = tail = newEntry;
        } else {
            memoryManager.setRight(tail, newEntry);
            memoryManager.setLeft(newEntry, tail);
            tail = newEntry;
        }
    }

    @Override
    public Map.Entry<K, V> getFirst() {
        if (head != MapDirectMemoryManager.NULL_ADDRESS) {
            V v = deserializeValue(memoryManager.getValue(head));
            return new OffHeapMapEntry<K, V>(deserializeKey(memoryManager.getKey(head)), v);
        } else {
            return null;
        }
    }

    @Override
    public int size() {
        return usedCount;
    }

    @Override
    public boolean put(K k, V v) {
        int index = k.hashCode() & mask;
        long root = hashEntries[index];
        long e = root;
        long p = root;
        ByteBuffer keyBuffer = keySerializer.serialize(k);
        while (e != MapDirectMemoryManager.NULL_ADDRESS) {
            if (memoryManager.isKey(e, keyBuffer)) {
                keyBuffer = memoryManager.copyKeyBuffer(keyBuffer);
                ByteBuffer value = valueSerializer.serialize(v);
                long newAddress = memoryManager.setKeyValue(keyBuffer, value);
                if (newAddress == MapDirectMemoryManager.NULL_ADDRESS) {
                    return false;
                }
                memoryManager.setNext(newAddress, memoryManager.getNext(e));
                removeFromLinkedList(e);
                memoryManager.free(e);
                addToLinkedList(k, v, newAddress);

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
        memoryManager.setNext(newEntry, hashEntries[index]);
        usedCount++;

        hashEntries[index] = newEntry;
        addToLinkedList(k, v, newEntry);
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

    private void removeFromLinkedList(long e) {
        long rightAddress = memoryManager.getRight(e);
        long leftAddress = memoryManager.getLeft(e);
        if (e == head) {
            head = rightAddress;
        } else {
            memoryManager.setRight(leftAddress, rightAddress);
        }
        if (e == tail) {
            tail = leftAddress;
        } else {
            memoryManager.setLeft(rightAddress, leftAddress);
        }
        memoryManager.setLeft(e, MapDirectMemoryManager.NULL_ADDRESS);
        memoryManager.setRight(e, MapDirectMemoryManager.NULL_ADDRESS);
    }

    @Override
    public Map.Entry<K, V> removeFirst() {
        if (head != MapDirectMemoryManager.NULL_ADDRESS) {
            V v = deserializeValue(memoryManager.getValue(head));
            K k = deserializeKey(memoryManager.getKey(head));
            removeEntry(k, head);
            return new OffHeapMapEntry<K, V>(k, v);
        } else {
            return null;
        }
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
        head = MapDirectMemoryManager.NULL_ADDRESS;
        tail = MapDirectMemoryManager.NULL_ADDRESS;
    }

    private static class Entry<K, V>implements Map.Entry<K,V> {
        private final K k;
        private final V v;
        private Entry(K k, V v) {
            this.k = k;
            this.v = v;
        }
        @Override
        public K getKey() {
            return k;
        }

        @Override
        public V getValue() {
            return v;
        }

        @Override
        public V setValue(V object) {
            throw new UnsupportedOperationException();
        }
    }
    
    private class EntryIterator implements Iterator<Map.Entry<K, V>> {
        private long next;
        private K currentKey;
        
        public EntryIterator() {
            next = head;
        }
        
        @Override
        public boolean hasNext() {
            return next != MapDirectMemoryManager.NULL_ADDRESS;
        }

        @Override
        public Map.Entry<K, V> next() {
            long address = next;
            next = memoryManager.getRight(address);
            currentKey = deserializeKey(memoryManager.getKey(address));
            V currentValue = deserializeValue(memoryManager.getValue(address));
            return new Entry(currentKey, currentValue);
        }

        @Override
        public void remove() {
            if (currentKey != null) {
                OffHeapLinkedHashMapImpl.this.remove(currentKey);
            }
        }
    }
    
    private class KeyIterator implements Iterator<K> {
        private long next;
        private K currentKey;
        public KeyIterator() {
            next = head;
        }
        
        @Override
        public boolean hasNext() {
            return next != MapDirectMemoryManager.NULL_ADDRESS;
        }

        @Override
        public K next() {
            long address = next;
            next = memoryManager.getRight(address);
            currentKey = deserializeKey(memoryManager.getKey(address));
            return currentKey;
        }

        @Override
        public void remove() {
            if (currentKey != null) {
                OffHeapLinkedHashMapImpl.this.remove(currentKey);
            }
        }
    }
    
    @Override
    public Iterator<K> keyIterator() {
        return new KeyIterator();
    }

    @Override
    public Iterator<Map.Entry<K, V>> entryIterator() {
        return new EntryIterator();
    }
}
