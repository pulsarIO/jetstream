/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */
@Sharable
public class HttpResponseHandler extends ChannelDuplexHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.http.netty.client");

    /**
     * @return the getTotalRcvCount
     */
    public static long getRcvCountPerSec() {
        return m_perSecRcvCount.getAndSet(0);
    }

    /**
     * @return the getTotalRcvCount
     */
    public static long getTotalRcvCount() {
        return m_totalRcvCount.get();
    }

    private final HttpClient m_httpClient;

    private static AtomicLong m_perSecRcvCount = new AtomicLong(0);

    private static AtomicLong m_totalRcvCount = new AtomicLong(0);

    public HttpResponseHandler(HttpClient httpClient) {
        m_httpClient = httpClient;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        printInfo("Rcvr Session created to host - " + ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName());

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        printInfo("EventProducerSessionHandler -> session closed to host - "
                + ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName());

        super.channelInactive(ctx);

        m_httpClient.channelDisconnected(ctx.channel());

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

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

    	if (m_perSecRcvCount.addAndGet(1) < 0)
    		m_perSecRcvCount.set(0);
    	if (m_totalRcvCount.addAndGet(1) < 0)
    		m_totalRcvCount.set(0);

    	HttpResponse response = (HttpResponse) msg;

    	if (response != null) {
    		String reqid = response.headers().get("X_EBAY_REQ_ID");
    		if (reqid != null) {
    			// we should only dispatch if the reqid is  set
    			m_httpClient.dispatchResponse(reqid, response);
    		}     
    		else {

    			ReferenceCountUtil.release(response);

    		}

    	}
    }
    
    
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            ctx.close();
        }
    }

    /**
     * @param message
     */
    private void printInfo(String message) {
        LOGGER.info( message);
    }
}
