/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class UnsignedIntEncoder {
    /**
     * Decode the integer from the byte buffer.
     * 
     * The input byte buffer should be readable, and it read from its current
     * position.
     * 
     * @param buffer
     * @return
     */
    public Integer decode(ByteBuffer buffer) {
        byte b = buffer.get();
        int result = b & 0x7F;
        if ((b & 0x80) != 0) {
            b = buffer.get();
            result |= (b & 0x7F) << 7;
            if ((b & 0x80) != 0) {
                b = buffer.get();
                result |= (b & 0x7F) << 14;
                if ((b & 0x80) != 0) {
                    b = buffer.get();
                    result |= (b & 0x7F) << 21;
                    if ((b & 0x80) != 0) {
                        b = buffer.get();
                        result |= (b & 0x7F) << 28;
                    }
                }
            }
        }
        return Integer.valueOf(result);
    }

    /**
     * Encode the integer to the buffer, the integer must be 0 or positive.
     * 
     * The input byte buffer is writable and it write from its current position.
     * 
     * If the input buffer capacity is not enough, the BufferOverflowException
     * will be thrown.
     * 
     * @param value
     * @param buffer
     * @throws BufferOverflowException
     *             when buffer capacity is not enough to write the string.
     */
    public void encode(Integer i, ByteBuffer buffer) throws BufferOverflowException {
        int value = i;
        if (value >>> 7 == 0) {
            buffer.put((byte) value);
        } else if (value >>> 14 == 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            buffer.put((byte) (value >>> 7));
        } else if (value >>> 21 == 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            buffer.put((byte) (value >>> 7 | 0x80));
            buffer.put((byte) (value >>> 14));
        } else if (value >>> 28 == 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            buffer.put((byte) (value >>> 7 | 0x80));
            buffer.put((byte) (value >>> 14 | 0x80));
            buffer.put((byte) (value >>> 21));
        } else {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            buffer.put((byte) (value >>> 7 | 0x80));
            buffer.put((byte) (value >>> 14 | 0x80));
            buffer.put((byte) (value >>> 21 | 0x80));
            buffer.put((byte) (value >>> 28));
        }
    }
}
