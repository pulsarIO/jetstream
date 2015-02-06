/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A general purpose encoder to encode primitive objects to byte buffer.
 * 
 * It supports all java primitive types and java.util.Date. The encoder is thread-safe.
 * 
 * @author xingwang
 *
 */
public class PrimitiveTLVEncoder {
    private final AsciiFirstStringEncoder stringEncoder = new AsciiFirstStringEncoder();
    private final UnsignedLongEncoder unsignedLongEncoder = new UnsignedLongEncoder();
    private final UnsignedIntEncoder unsignedIntEncoder = new UnsignedIntEncoder();
    
    private static final byte TYPE_NULL = 127;
    private static final byte TYPE_STRING = 0;
    private static final byte TYPE_CHAR = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_INT = 3;
    private static final byte TYPE_SHORT = 4;
    private static final byte TYPE_BYTE = 5;
    private static final byte TYPE_DOUBLE = 6;
    private static final byte TYPE_FLOAT = 7;
    private static final byte TYPE_BOOLEAN_TRUE = 8;
    private static final byte TYPE_BOOLEAN_FALSE = 9;
    private static final byte TYPE_DATE = 10;
    private static final byte TYPE_LIST = 11;
    private static final byte TYPE_SET = 12;
    private static final byte TYPE_MAP = 13;
    private static final byte TYPE_UNSIGNED_INT = 14;
    private static final byte TYPE_UNSIGNED_LONG = 15;
    /**
     * Decode the object from byte buffer.
     * 
     * The input buffer should be readable.
     * 
     * @param buffer
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object decode(ByteBuffer buffer) {
        byte type = buffer.get();
        switch (type) {
        case TYPE_NULL:
            return null;
        case TYPE_LIST:
        {
            int length = buffer.getInt();
            List l = new ArrayList(length);
            for (int i = 0; i < length; i++) {
                l.add(decode(buffer));
            }
            return l;
        }
        case TYPE_SET:
        {
            int length = buffer.getInt();
            Set l = new HashSet(length);
            for (int i = 0; i < length; i++) {
                l.add(decode(buffer));
            }
            return l;
        }
        case TYPE_MAP:
        {
            int length = buffer.getInt();
            Map l = new HashMap(length);
            for (int i = 0; i < length; i++) {
                l.put(decode(buffer), decode(buffer));
            }
            return l;
        }
        case TYPE_STRING:
            return stringEncoder.decode(buffer);
        case TYPE_BOOLEAN_TRUE: 
            return  Boolean.TRUE;
        case TYPE_BOOLEAN_FALSE:     
            return Boolean.FALSE;
        case TYPE_UNSIGNED_LONG:
            return unsignedLongEncoder.decode(buffer);
        case TYPE_UNSIGNED_INT:
            return unsignedIntEncoder.decode(buffer);
        case TYPE_LONG:
            return Long.valueOf(buffer.getLong());
        case TYPE_INT:
            return Integer.valueOf(buffer.getInt());
        case TYPE_SHORT:
            return Short.valueOf(buffer.getShort());
        case TYPE_BYTE:
            return Byte.valueOf(buffer.get());
        case TYPE_DOUBLE:
            return Double.valueOf(buffer.getDouble());
        case TYPE_FLOAT:
            return Float.valueOf(buffer.getFloat());
        case TYPE_CHAR:
            return Character.valueOf(buffer.getChar());
        case TYPE_DATE:
            return new Date(buffer.getLong());
        default:
            throw new IllegalArgumentException("Invalid type " + type);
        }
    }
    
    /**
     * Encode the object to the byte buffer.
     * 
     * The input buffer should be writable.
     * 
     * @param value
     * @param buffer
     * 
     * @throws BufferOverflowException when buffer capacity is not enough to write.
     */
    public void encode(Object value, ByteBuffer buffer) throws BufferOverflowException {
        if (value == null) {
            buffer.put(TYPE_NULL);
            return;
        }
        if (String.class.equals(value.getClass())) {
            buffer.put(TYPE_STRING);
            stringEncoder.encode((String) value, buffer);
        } else if (List.class.isAssignableFrom(value.getClass())) {
            buffer.put(TYPE_LIST);
            List<?> l = (List<?>) value;
            buffer.putInt(l.size());
            for (int i = 0, t = l.size(); i < t; i ++) {
                encode(l.get(i), buffer);
            }
        } else if (Set.class.isAssignableFrom(value.getClass())) {
            buffer.put(TYPE_SET);
            Set<?> l = (Set<?>) value;
            buffer.putInt(l.size());
            for (Object x : l) {
                encode(x, buffer);
            }
        } else if (Map.class.isAssignableFrom(value.getClass())) {
            buffer.put(TYPE_MAP);
            Map<?,?> l = (Map<?, ?>) value;
            buffer.putInt(l.size());
            for (Map.Entry<?, ?> x : l.entrySet()) {
                encode(x.getKey(), buffer);
                encode(x.getValue(), buffer);
            }
        } else if (Boolean.class.equals(value.getClass()) || boolean.class.equals(value.getClass())) {
            if (Boolean.TRUE.equals(value)) {
                buffer.put(TYPE_BOOLEAN_TRUE);
            } else {
                buffer.put(TYPE_BOOLEAN_FALSE);
            }
        } else if (Long.class.equals(value.getClass()) || long.class.equals(value.getClass())) {
            if (((Long) value) >= 0) {
                buffer.put(TYPE_UNSIGNED_LONG);
                unsignedLongEncoder.encode((Long) value, buffer);
            } else {
                buffer.put(TYPE_LONG);
                buffer.putLong((Long) value);
            }
        } else if (Integer.class.equals(value.getClass()) || int.class.equals(value.getClass())) {
            if ((Integer) value >= 0) {
                buffer.put(TYPE_UNSIGNED_INT);
                unsignedIntEncoder.encode((Integer) value, buffer);
            } else {
                buffer.put(TYPE_INT);
                buffer.putInt((Integer) value);
            }
        } else if (Short.class.equals(value.getClass()) || short.class.equals(value.getClass())) {
            buffer.put(TYPE_SHORT);
            buffer.putShort((Short) value);
        } else if (Byte.class.equals(value.getClass()) || byte.class.equals(value.getClass())) {
            buffer.put(TYPE_BYTE);
            buffer.put((Byte) value);
        } else if (Double.class.equals(value.getClass()) || double.class.equals(value.getClass())) {
            buffer.put(TYPE_DOUBLE);
            buffer.putDouble((Double) value);
        } else if (Float.class.equals(value.getClass()) || float.class.equals(value.getClass())) {
            buffer.put(TYPE_FLOAT);
            buffer.putFloat((Float) value);
        } else if (Character.class.equals(value.getClass()) || char.class.equals(value.getClass())) {
            buffer.put(TYPE_CHAR);
            buffer.putChar((Character) value);
        } else if (Date.class.equals(value.getClass())) {
            buffer.put(TYPE_DATE);
            buffer.putLong(((Date) value).getTime());
        } else {
            throw new IllegalArgumentException("Can not support non-primitive type:" + value.getClass());
        }
    }
}
