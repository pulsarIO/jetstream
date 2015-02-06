/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.nio.ByteBuffer;

/**
 * A serializer interface for object serialization when used in off-heap
 * collections.
 * 
 * The output bytebuffer can be reused in local thread.
 * 
 * @author xingwang
 * 
 * @param <V>
 */
public interface OffHeapSerializer<V> {
    /**
     * De-serialize the byte buffer to object. The input data buffer will be
     * reused and the implementation should copy bytes rather than reference the
     * data on the buffer.
     * 
     * @param data
     * @return
     */
    V deserialize(ByteBuffer data, int pos, int length);

    /**
     * Serialize the object and return it as a readable byte buffer.`
     * 
     * The returned buffer can be reused when off heap write operation finished.
     * 
     * @param v
     * @return
     */
    ByteBuffer serialize(V v);
}
