/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;

import com.ebay.jetstream.util.Request;


public abstract class AbstractHttpRequest extends Request{

	
    protected void postResponse(Channel channel, HttpRequest request, HttpResponse response) {

        boolean keepAlive = HttpHeaders.isKeepAlive(request);

        String reqid = request.headers().get("X_EBAY_REQ_ID");

        if (reqid != null && !reqid.isEmpty()) // if request contains
                                               // X_EBAY_REQ_ID we will echo it
                                               // back
            HttpHeaders.setHeader(response, "X_EBAY_REQ_ID", reqid);

        // we will echo back cookies for now in the response. Our request ID is
        // set as a cookie

        String contentLenHeader = response.headers().get(HttpHeaders.Names.CONTENT_LENGTH);
        if (contentLenHeader == null)
            HttpHeaders.setContentLength(response, 0);

        // Write the response.
        ChannelFuture future = channel.writeAndFlush(response);

        // Close the non-keep-alive connection after the write operation is
        // done.
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
