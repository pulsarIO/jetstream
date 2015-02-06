/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;

class ObjectSerializer<V> implements OffHeapSerializer<V> {
    private static ThreadLocal<ByteArrayOutputStream> outputStreamHolder = new ThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
            return new ByteArrayOutputStream() {
                public synchronized byte toByteArray()[] { //override to avoid copy. 
                    return buf;
                }
            };
        }
    };

    @SuppressWarnings("unchecked")
    @Override
    public V deserialize(ByteBuffer buf, int pos, int length) {
        ByteArrayInputStream in = new ByteArrayInputStream(buf.array());
        ObjectInputStream ios = null;
        try {
            ios = new ObjectInputStream(in);
            return (V) ios.readObject();
        } catch (IOException e) {
            throw new IllegalArgumentException("Fail to deserialize the object", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Fail to deserialize the object", e);
        } finally {
            if (ios != null) {
                try {
                    ios.close();
                } catch (IOException e) {
                }
            }
        }
    }


    @Override
    public ByteBuffer serialize(V o) {
        ByteArrayOutputStream out = outputStreamHolder.get();
        out.reset();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(out);
            oos.writeObject(o);
        } catch (IOException e) {
            throw new IllegalArgumentException("Fail to serializer the object of class + " + o.getClass(), e);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException e) {
                }
            }
        }
        int count = out.size();
        return ByteBuffer.wrap(out.toByteArray(), 0, count);
    }

}
