/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer;

import java.nio.ByteBuffer;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;

/**
 * An offheap serializer implementation for Integer.
 * 
 * @author xingwang
 * 
 */
class IntSerializer implements OffHeapSerializer<Integer> {
    private static ThreadLocal<ByteBuffer> keyBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(4);
        }
    };

    @Override
    public Integer deserialize(ByteBuffer data, int pos, int length) {
        return Integer.valueOf(data.getInt());
    }

    @Override
    public ByteBuffer serialize(Integer k) {
        ByteBuffer buffer = keyBuffer.get();
        buffer.clear();
        buffer.putInt(k);
        buffer.flip();
        return buffer;
    }

}
