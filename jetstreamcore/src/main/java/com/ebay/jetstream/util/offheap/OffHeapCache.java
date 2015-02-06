/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.util.Map.Entry;

/**
 * Off heap linked cache interface, the read/write is O(1) and
 * the data will be linked via its expiration time.
 * 
 * It is similar with Map but remove some unnecessary overhead.
 * 
 * @author xingwang
 *
 * @param <K>
 * @param <V>
 */
public interface OffHeapCache<K,V> {

    /**
     * Remove the first entry and return.
     * 
     * @return
     */
    Entry<K, V> removeFirst();

    /**
     * Remove the entry from the cache.
     * 
     * @param k
     * @return false when the k is not in the cache.
     * 
     */
    boolean remove(K k);

    /**
     * Add new entry to the cache.
     * 
     * The expiration time should be greater than current time and less
     * than the max expiration time defined by the expiration time slots.
     * 
     * @param k
     * @param v
     * @param expiredTime
     * @return
     */
    boolean put(K k, V v, long expiredTime);

    /**
     * The size of the cache.
     * 
     * @return
     */
    int size();

    /**
     * Return the first entry.
     * 
     * @return
     */
    Entry<K, V> getFirst();

    /**
     * Return the value by key. Return null if not in the cache.
     * 
     * @param k
     * @return
     */
    V get(K k);

    /**
     * Remove the first entry which expiration time less than the input time.
     * 
     * And once this method called, the caller should never put entry which expiration
     * time less than the input time.
     * 
     * @param expirationTime
     * @return
     */
    Entry<K, V> removeExpiredData(long expirationTime);

    /**
     * Clear the map.
     */
    void clear();
}
