/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer;

import java.nio.ByteBuffer;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;

/**
 * An offheap serializer implementation for Long.
 * 
 * @author xingwang
 * 
 */
class LongSerializer implements OffHeapSerializer<Long> {
    private static ThreadLocal<ByteBuffer> keyBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(8);
        }
    };

    @Override
    public Long deserialize(ByteBuffer data, int pos, int length) {
        return Long.valueOf(data.getLong());
    }

    @Override
    public ByteBuffer serialize(Long k) {
        ByteBuffer buffer = keyBuffer.get();
        buffer.clear();
        buffer.putLong(k);
        buffer.flip();
        return buffer;
    }

}
