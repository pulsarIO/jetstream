/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.serializer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.util.Attribute;

import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventProducer;


public class NettyObjectEncoder extends ObjectEncoder {

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

		Attribute<Boolean> attr = ctx.channel().attr(EventProducer.m_kskey);
		
		Boolean enableKryo = attr.get();
		
		if ((enableKryo != null) && (enableKryo == true))
			ctx.write(msg, promise);
		else
			super.write(ctx, msg, promise);
		
		
	}
}
