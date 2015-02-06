/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
/**
 * An off heap linked hash map without unnecessary overhead on Map interface.
 * 
 * @author xingwang
 *
 * @param <K>
 * @param <V>
 */
public interface OffHeapLinkedHashMap<K,V> {
    /**
     * Remove the first entry.
     * 
     * @return
     */
    Entry<K, V> removeFirst();
    
    /**
     * Add an iterator for access keys by insertion order. 
     * 
     * @return
     */
    Iterator<K> keyIterator();
    
    /**
     * Add an iterator for access entries by insertion order. 
     * 
     * @return
     */
    Iterator<Map.Entry<K, V>> entryIterator();

    /**
     * Remove entry by key.
     * 
     * @param k
     * @return
     */
    boolean remove(K k);

    /**
     * Put an new entry and it will be put on the tail of the map.
     * 
     * @param k
     * @param v
     * @return
     */
    boolean put(K k, V v);

    /**
     * Return size.
     * 
     * @return
     */
    int size();

    /**
     * Return first entry.
     * 
     * @return
     */
    Entry<K, V> getFirst();

    /**
     * Return value by key.
     * 
     * @param k
     * @return
     */
    V get(K k);

    /**
     * Clear the map.
     */
    void clear();
}
