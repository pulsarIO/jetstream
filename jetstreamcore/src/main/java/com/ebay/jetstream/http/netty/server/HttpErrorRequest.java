/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class HttpErrorRequest extends AbstractHttpRequest {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.rtbd.http.netty.server");
    private final HttpRequest m_request;
    private final Channel m_channel;
    private final HttpResponseStatus m_status;

    public HttpErrorRequest(HttpRequest req, Channel channel, HttpResponseStatus status) {
        m_request = req;
        m_channel = channel;
        m_status = status;
    }
    
    public void processRequest() {
        try {
            DefaultHttpServletResponse servletResponse = new DefaultHttpServletResponse(HttpVersion.HTTP_1_1, m_status, 0);
            servletResponse.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

            try {
                servletResponse.flushBuffer();
                postResponse(m_channel, m_request, servletResponse.toNettyHttpResponse());

            } catch (Throwable e) {
                LOGGER.error( e.getMessage(), e);
            }
        } finally {
            ReferenceCountUtil.release(m_request);
        }
    }

    @Override
    public boolean execute() {

        try {
            processRequest();
        } catch (Throwable t) {
            LOGGER.error( "Execption while dispatching Http request downstream " + t.getLocalizedMessage());
        }
        
        return true;
    }
}
