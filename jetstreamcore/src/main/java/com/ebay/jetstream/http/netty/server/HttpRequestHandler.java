/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.timeout.IdleStateEvent;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.messaging.MessageServiceTimer;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */
@Sharable
public class HttpRequestHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.http.netty.server");

    /**
     * @return the getTotalRcvCount
     */
    public long getRcvCountPerSec() {
        return m_perSecRcvCount.get();
    }

    /**
     * @return the getTotalRcvCount
     */
    public long getTotalRcvCount() {
        return m_totalRcvCount.get();
    }
    
    /**
     * @return the get total content length
     */
    public long getTotalContentLength() {
        return m_totalContentLength.get();
    }


    private HttpServer m_server = null;
    private LongEWMACounter m_perSecRcvCount = new LongEWMACounter(60, MessageServiceTimer
            .sInstance().getTimer());
    private LongCounter m_totalRcvCount = new LongCounter();
    private LongCounter m_totalContentLength = new LongCounter();

    public HttpRequestHandler(HttpServer ec) {
        m_server = ec;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        if (LOGGER.isDebugEnabled()) {
            debug("Rcvr Session created to host - " + ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName());
        }

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        if (LOGGER.isDebugEnabled()) {
            debug("EventProducerSessionHandler -> session closed to host - "
                    + ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName());
        }

        super.channelInactive(ctx);

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            m_perSecRcvCount.increment();
        if (m_totalRcvCount.addAndGet(1) < 0)
            m_totalRcvCount.increment();

        processHttpRequest((HttpRequest) msg, ctx);

    }

    private void debug(String message) {
        LOGGER.debug(message);
    }

    private void debugHeadersAndCookies(HttpRequest request) {

        StringBuilder headersandaccokies = new StringBuilder();

        // echo the header for now
        for (Map.Entry<String, String> h : request.headers()) {
            headersandaccokies.append("HEADER: " + h.getKey() + " = " + h.getValue() + "\r\n");
        }
        headersandaccokies.append("\r\n");

        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        if (!params.isEmpty()) {
            for (Entry<String, List<String>> p : params.entrySet()) {
                String key = p.getKey();
                List<String> vals = p.getValue();
                for (String val : vals) {
                    headersandaccokies.append("PARAM: " + key + " = " + val + "\r\n");
                }
            }
            headersandaccokies.append("\r\n");
        }

        debug(headersandaccokies.toString());

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ee) throws Exception {

        String message = "Exception Caught while communicating to ";

        try {
            message += ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName();
            message += ee.getCause();
            printInfo(message);
        } catch (Throwable t) {
        }

        printInfo("EventProducerSessionHandler -> exceptionCaught" + ee.toString());

        super.exceptionCaught(ctx, ee);
    }

    /**
     * @param message
     */
    private void printInfo(String message) {
        LOGGER.info( message);
    }

    private void processHttpRequest(HttpRequest message, ChannelHandlerContext ctx) throws Exception {

        if (LOGGER.isDebugEnabled()) {
            debugHeadersAndCookies(message);
        }

        // Expect: 100-continue should be handled by HttpObjectAggregator.
        ByteBuf buf = ((FullHttpMessage) message).content();
        m_totalContentLength.addAndGet(buf.readableBytes());
        m_server.processHttpRequest(message, ctx.channel());
    }
    
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            ctx.close();
        }
    }

}
