/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KeepAliveHandler extends ChannelDuplexHandler {
    private static final AttributeKey<Long> CREATION_TIME = AttributeKey.valueOf("Http_Connection_Creation_Time");
    private static final AttributeKey<AtomicInteger> REQUEST_COUNT = AttributeKey.valueOf("Http_Request_Count");

    private final int maxKeepAliveRequests;
    private final long maxKeepAliveTimeoutInNanoTime;

    public KeepAliveHandler(int maxKeepAliveRequests, int maxKeepAliveTimeout) {
        this.maxKeepAliveRequests = maxKeepAliveRequests;
        this.maxKeepAliveTimeoutInNanoTime = TimeUnit.NANOSECONDS.convert(maxKeepAliveTimeout, TimeUnit.SECONDS);

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.attr(CREATION_TIME).set(System.nanoTime());
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Attribute<AtomicInteger> attr = ctx.attr(REQUEST_COUNT);
        if (attr.get() == null) {
            attr.setIfAbsent(new AtomicInteger());
        }
        attr.get().incrementAndGet();
        ctx.fireChannelRead(msg);
    }

    private void closeConnection(Object msg, ChannelPromise promise) {
        if (msg instanceof HttpResponse) {
            HttpHeaders.setHeader((HttpResponse) msg, HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        }
        promise.addListener(ChannelFutureListener.CLOSE);
    }
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (maxKeepAliveTimeoutInNanoTime > 0 && ((System.nanoTime() - ctx.attr(CREATION_TIME).get()) > maxKeepAliveTimeoutInNanoTime)) {
            closeConnection(msg, promise);
        } else if (maxKeepAliveRequests > 0 && (ctx.attr(REQUEST_COUNT).get() != null && ctx.attr(REQUEST_COUNT).get().get() >= maxKeepAliveRequests)) {
            closeConnection(msg, promise);
        }
        ctx.write(msg, promise);
    }
}
