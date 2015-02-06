/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
/**
 * This is a string encoder to encode/decode string  to byte buffer.
 * 
 * It is optimized for ascii string, and will treat all input strings
 * as ascii string first, only write UTF-8 string when there are non-ascii
 * characters in the string. The encoder is thread-safe.
 * 
 * @author xingwang
 *
 */
public final class AsciiFirstStringEncoder {
    /**
     * Decode the string from the byte buffer.
     * 
     * The input byte buffer should be readable, and it read from its current position.
     * 
     * @param buffer
     * @return
     */
    public String decode(ByteBuffer buffer) {
        byte b = buffer.get();
        if ((b & 0x80) == 0) {
            return readAscii(buffer);
        } else {
            int charCount = readLength(b, buffer);
            switch (charCount) {
            case 0:
                return null;
            case 1:
                return "";
            }
            charCount--;
            char[] chars = new char[charCount];
            readAsciiFirst(charCount, chars, buffer);
            return new String(chars, 0, charCount);
        }
    }

    /**
     * Encode the string to the buffer, null and empty string is allowable.
     * 
     * The input byte buffer is writable and it write from its current position.
     * 
     * If the input buffer capacity is not enough, the BufferOverflowException will be thrown.
     * 
     * @param value
     * @param buffer
     * @throws BufferOverflowException when buffer capacity is not enough to write the string.
     */
    public void encode(String value, ByteBuffer buffer) throws BufferOverflowException {
        if (value == null) {
            buffer.put((byte) (0x80)); // 0 means null, bit 8 means UTF8.
            return;
        }
        int charCount = value.length();
        if (charCount == 0) {
            buffer.put((byte) (1 | 0x80)); // 1 means empty string, bit 8 means
                                           // UTF8.
            return;
        }
        int position = buffer.position();
        boolean ascii = true;
        if (charCount > 1) {
            for (int i = 0; i < charCount; i++) {
                int c = value.charAt(i);
                if (c > 127) {
                    ascii = false;
                    break;
                }
                buffer.put((byte) c);
            }
        } else {
            ascii = false;
        }

        if (ascii) {
            position = buffer.position();
            buffer.put(position - 1, (byte) (buffer.get(position - 1) | 0x80));
        } else {
            buffer.position(position);
            writeLength(charCount + 1, buffer);
            int charIndex = 0;
            for (; charIndex < charCount; charIndex++) {
                int c = value.charAt(charIndex);
                if (c > 127)
                    break;
                buffer.put((byte) c);
            }
            writeUTF8String(value, charCount, charIndex, buffer);
        }

    }

    private String readAscii(ByteBuffer buffer) {
        int start = buffer.position() - 1;
        byte b;
        do {
            b = buffer.get();
        } while ((b & 0x80) == 0);
        int end = buffer.position();
        buffer.put(end - 1, (byte) (b & 0x7F));
        String value = new String(buffer.array(), start, end - start);
        return value;
    }

    private void readAsciiFirst(int charCount, char[] chars, ByteBuffer buffer) {
        // Try to read 7 bit ASCII chars.
        int charIndex = 0;
        int b;
        while (charIndex < chars.length) {
            b = buffer.get();
            if (b < 0) {
                buffer.position(buffer.position() - 1);
                break;
            }
            chars[charIndex++] = (char) b;
        }
        // If buffer didn't hold all chars or any were not ASCII, use slow path
        // for remainder.
        if (charIndex < charCount)
            readUTF8String(charCount, charIndex, chars, buffer);
    }

    private int readLength(int b, ByteBuffer buffer) {
        int result = b & 0x3F; // Mask all but first 6 bits.
        if ((b & 0x40) != 0) { // Bit 7 means another byte, bit 8 means UTF8.
            b = buffer.get();
            result |= (b & 0x7F) << 6;
            if ((b & 0x80) != 0) {
                b = buffer.get();
                result |= (b & 0x7F) << 13;
                if ((b & 0x80) != 0) {
                    b = buffer.get();
                    result |= (b & 0x7F) << 20;
                    if ((b & 0x80) != 0) {
                        b = buffer.get();
                        result |= (b & 0x7F) << 27;
                    }
                }
            }
        }
        return result;
    }



    private void readUTF8String(int charCount, int charIndex, char[] chars, ByteBuffer buffer) {
        while (charIndex < charCount) {
            int b = buffer.get() & 0xFF;
            switch (b >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                chars[charIndex] = (char) b;
                break;
            case 12:
            case 13:
                chars[charIndex] = (char) ((b & 0x1F) << 6 | buffer.get() & 0x3F);
                break;
            case 14:
                chars[charIndex] = (char) ((b & 0x0F) << 12 | (buffer.get() & 0x3F) << 6 | buffer.get() & 0x3F);
                break;
            }
            charIndex++;
        }
    }

    private void writeLength(int value, ByteBuffer buffer) {
        if (value >>> 6 == 0) {
            buffer.put((byte) (value | 0x80)); // Set bit 8.
        } else if (value >>> 13 == 0) {
            buffer.put((byte) (value | 0x40 | 0x80)); // Set bit 7 and 8.
            buffer.put((byte) (value >>> 6));
        } else if (value >>> 20 == 0) {
            buffer.put((byte) (value | 0x40 | 0x80)); // Set bit 7 and 8.
            buffer.put((byte) ((value >>> 6) | 0x80)); // Set bit 8.
            buffer.put((byte) (value >>> 13));
        } else if (value >>> 27 == 0) {
            buffer.put((byte) (value | 0x40 | 0x80)); // Set bit 7 and 8.
            buffer.put((byte) ((value >>> 6) | 0x80)); // Set bit 8.
            buffer.put((byte) ((value >>> 13) | 0x80)); // Set bit 8.
            buffer.put((byte) (value >>> 20));
        } else {
            buffer.put((byte) ((value >>> 6) | 0x80)); // Set bit 8.
            buffer.put((byte) ((value >>> 13) | 0x80)); // Set bit 8.
            buffer.put((byte) ((value >>> 20) | 0x80)); // Set bit 8.
            buffer.put((byte) (value >>> 27));
        }
    }

    private void writeUTF8String(CharSequence value, int charCount, int charIndex, ByteBuffer buffer) {
        for (; charIndex < charCount; charIndex++) {
            int c = value.charAt(charIndex);
            if (c <= 0x007F) {
                buffer.put((byte) c);
            } else if (c > 0x07FF) {
                buffer.put((byte) (0xE0 | c >> 12 & 0x0F));
                buffer.put((byte) (0x80 | c >> 6 & 0x3F));
                buffer.put((byte) (0x80 | c & 0x3F));
            } else {
                buffer.put((byte) (0xC0 | c >> 6 & 0x1F));
                buffer.put((byte) (0x80 | c & 0x3F));
            }
        }
    }
}
