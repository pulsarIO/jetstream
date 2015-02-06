/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer;

import java.nio.ByteBuffer;

import com.ebay.jetstream.messaging.messagetype.Any;
import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer;

public class KryoSerializer<V> implements OffHeapSerializer<V> {
    private static class KryoContext {
        private Kryo kryo = new Kryo();
        private Output out;
        private int bufferSize = 4096;
        
        public KryoContext() {
            out = new Output(bufferSize);
            kryo.register(Any.class, new KryoSerializableSerializer());
            try {
                kryo.register(Class.forName("com.ebay.jetstream.event.JetstreamEvent"), new KryoSerializableSerializer());
            } catch (ClassNotFoundException e) {
            }
        }
        
        public Kryo getKryo() {
            return kryo;
        }
        public Output getOut() {
            return out;
        }
        
        public Output expandOut() {
            bufferSize = bufferSize * 2;
            this.out = new Output(bufferSize);
            return out;
            
        }
    }
    private static ThreadLocal<KryoContext> kryoContextHolder = new ThreadLocal<KryoContext>() {
        @Override
        protected KryoContext initialValue() {
            return new KryoContext();
        }
    };
    
    
    @Override
    public V deserialize(ByteBuffer data, int pos, int length) {
        KryoContext kryoContext = kryoContextHolder.get();
        Kryo kryo = kryoContext.getKryo();
        
        Input input = new Input(data.array(), pos, length);
        return (V) kryo.readClassAndObject(input);
    }

    @Override
    public ByteBuffer serialize(V v) {
        KryoContext kryoContext = kryoContextHolder.get();
        Kryo kryo = kryoContext.getKryo();
        Output out = kryoContext.getOut();
        out.clear();
        while (true) {
            try {
                kryo.writeClassAndObject(out, v);
                break;
            } catch (KryoException ex) {
                if (ex.getMessage() != null && ex.getMessage().startsWith("Buffer overflow.")) {
                    out = kryoContext.expandOut();
                } else {
                    throw ex;
                }
            }
        }
        return ByteBuffer.wrap(out.toBytes());
    }

}
