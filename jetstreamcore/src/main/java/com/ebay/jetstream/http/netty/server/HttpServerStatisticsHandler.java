/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import com.ebay.jetstream.counter.LongCounter;

@Sharable
public class HttpServerStatisticsHandler  extends ChannelInboundHandlerAdapter {
    private LongCounter bytesCounter = new LongCounter();
    
    public long getBytesRead() {
        return bytesCounter.get();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        bytesCounter.addAndGet(((ByteBuf) msg).readableBytes());
        super.channelRead(ctx, msg);
    }
}
