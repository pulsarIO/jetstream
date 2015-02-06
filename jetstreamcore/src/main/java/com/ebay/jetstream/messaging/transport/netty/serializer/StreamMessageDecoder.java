/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.serializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.serialization.ClassResolver;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.ObjectStreamConstants;
import java.io.StreamCorruptedException;

import com.ebay.jetstream.messaging.transport.netty.eventconsumer.EventProducerSessionHandler;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class StreamMessageDecoder extends LengthFieldBasedFrameDecoder {
    static final byte KRYO_STREAM_VERSION = (byte) 0x00; 
    static final byte JAVA_STREAM_VERSION = (byte) ObjectStreamConstants.STREAM_VERSION; // Java is 0x5
    
    // Copied from Netty code due to it is package visible
    private static class CompactObjectInputStream extends ObjectInputStream {
        static final int TYPE_FAT_DESCRIPTOR = 0;
        static final int TYPE_THIN_DESCRIPTOR = 1;

        private final ClassResolver classResolver;

        CompactObjectInputStream(InputStream in, ClassResolver classResolver) throws IOException {
            super(in);
            this.classResolver = classResolver;
        }

        @Override
        protected void readStreamHeader() throws IOException {
            int version = readByte() & 0xFF;
            if (version != STREAM_VERSION) {
                throw new StreamCorruptedException("Unsupported version: " + version);
            }
        }

        @Override
        protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
            int type = read();
            if (type < 0) {
                throw new EOFException();
            }
            switch (type) {
            case TYPE_FAT_DESCRIPTOR:
                return super.readClassDescriptor();
            case TYPE_THIN_DESCRIPTOR:
                String className = readUTF();
                Class<?> clazz = classResolver.resolve(className);
                return ObjectStreamClass.lookupAny(clazz);
            default:
                throw new StreamCorruptedException("Unexpected class descriptor type: " + type);
            }
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            Class<?> clazz;
            try {
                clazz = classResolver.resolve(desc.getName());
            } catch (ClassNotFoundException ex) {
                clazz = super.resolveClass(desc);
            }

            return clazz;
        }

    }

    private ClassResolver classResolver;

    private static class KryoContext {
        private Kryo kryo = new Kryo();

        public KryoContext() {
        }

        public Kryo getKryo() {
            return kryo;
        }
    }

    private static ThreadLocal<KryoContext> kryoContextHolder = new ThreadLocal<KryoContext>() {
        @Override
        protected KryoContext initialValue() {
            return new KryoContext();
        }
    };

    /**
     * Creates a new decoder whose maximum object size is {@code 1048576} bytes.
     * If the size of the received object is greater than {@code 1048576} bytes,
     * a {@link StreamCorruptedException} will be raised.
     */
    public StreamMessageDecoder(ClassResolver classResolver) {
        this(classResolver, 1048576);
    }

    /**
     * Creates a new decoder with the specified maximum object size.
     * 
     * @param maxObjectSize
     *            the maximum byte length of the serialized object. if the
     *            length of the received object is greater than this value,
     *            {@link StreamCorruptedException} will be raised.
     */
    public StreamMessageDecoder(ClassResolver classResolver, int maxObjectSize) {
        super(maxObjectSize, 0, 4, 0, 4);
        this.classResolver = classResolver;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ctx.fireChannelRead(EventProducerSessionHandler.BATCH_START_EVENT);
        try {
            super.channelRead(ctx, msg);
        } finally {
            ctx.fireChannelRead(EventProducerSessionHandler.BATCH_END_EVENT);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        int readerIndex = frame.readerIndex();
        byte version = frame.readByte();
        frame.readerIndex(readerIndex);
        
        if (version == KRYO_STREAM_VERSION) {
            return decodeAsKryo(frame);
        } else {
            return new CompactObjectInputStream(
                    new ByteBufInputStream(frame), classResolver).readObject();
        }
    }

    private Object decodeAsKryo(ByteBuf frame) {
        Kryo kryo = kryoContextHolder.get().getKryo();
        Input input = new Input(new ByteBufInputStream(frame));
        input.readByte(); // skip first byte
        Object object = kryo.readClassAndObject(input);
        return object;
    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.slice(index, length);
    }

}
