/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.util.AsciiFirstStringEncoder;

/**
 * An offheap serializer implementation for String.
 * 
 * It optimized when the string is ascii string and it is thread-safe. The byte
 * buffer will be reused in thread local manner.
 * 
 * @author xingwang
 * 
 */
class StringSerializer implements OffHeapSerializer<String> {
    private static ThreadLocal<ByteBuffer> keyBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(1024);
        }
    };

    private final AsciiFirstStringEncoder encoder = new AsciiFirstStringEncoder();

    @Override
    public String deserialize(ByteBuffer data, int pos, int length) {
        return encoder.decode(data);
    }

    @Override
    public ByteBuffer serialize(String k) {
        ByteBuffer buffer = keyBuffer.get();
        buffer.clear();
        while (true) {
            try {
                encoder.encode(k, buffer);
                buffer.flip();
                break;
            } catch (BufferOverflowException ex) {
                buffer = ByteBuffer.allocate(buffer.capacity() * 2);
                keyBuffer.set(buffer);
            }
        }
        return buffer;
    }

}
