/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.serializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.Attribute;

import java.io.Serializable;

import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventProducer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;


public class KryoObjectEncoder extends MessageToByteEncoder<Serializable> {
    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];
    
    private static class KryoContext {
        private Kryo kryo = new Kryo();
        private Output out;
        
        public KryoContext() {
            out = new Output(4096);
        }
        public Kryo getKryo() {
            return kryo;
        }
        public Output getOut() {
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
    protected void encode(ChannelHandlerContext ctx, Serializable msg, ByteBuf out) throws Exception {
        KryoContext kryoContext = kryoContextHolder.get();
        Kryo kryo = kryoContext.getKryo();
        Output output = kryoContext.getOut();
        output.clear();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        int startIdx = out.writerIndex();
        bout.write(LENGTH_PLACEHOLDER);
        output.setOutputStream(bout);
        output.writeByte(StreamMessageDecoder.KRYO_STREAM_VERSION);
        kryo.writeClassAndObject(output, msg);
        output.flush();
        bout.flush();
        bout.close();
        output.close();
        
        int endIdx = out.writerIndex();

        out.setInt(startIdx, endIdx - startIdx - 4);
    }
    
    @Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

		

		Attribute<Boolean> attr = ctx.channel().attr(EventProducer.m_kskey);
		
		Boolean enableKryo = attr.get();
		
		if ((enableKryo != null) && (enableKryo == true))
			super.write(ctx, msg, promise);
		else
			ctx.write(msg, promise);
		
		
	}
    
}