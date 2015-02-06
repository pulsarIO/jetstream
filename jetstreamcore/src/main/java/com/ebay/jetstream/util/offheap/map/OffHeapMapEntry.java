/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.map;

import java.util.Map;

final class OffHeapMapEntry<K, V> implements Map.Entry<K, V> {
    private final K k;
    private final V v;

    public OffHeapMapEntry(K k, V v) {
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

    @Override
    public String toString() {
        return "OffHeapMapEntry [k=" + k + ", v=" + v + "]";
    }

    @Override
    public int hashCode() {
        return k.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        OffHeapMapEntry<K, V> other = (OffHeapMapEntry<K, V>) obj;
        if (k == null) {
            if (other.k != null)
                return false;
        } else if (!k.equals(other.k))
            return false;
        if (v == null) {
            if (other.v != null)
                return false;
        } else if (!v.equals(other.v))
            return false;
        return true;
    }

}
