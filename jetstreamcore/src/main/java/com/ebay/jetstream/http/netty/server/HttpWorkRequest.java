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

import java.io.IOException;

import javax.servlet.http.HttpServlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class HttpWorkRequest extends AbstractHttpRequest {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.rtbd.http.netty.server");
    private final HttpRequest m_request;
    private final Channel m_channel;
    private final HttpServlet m_servlet;
    private final HttpServerConfig m_serverConfig;

    public HttpWorkRequest(HttpRequest req, Channel channel, HttpServlet servlet, HttpServerConfig serverConfig) {
        m_request = req;
        m_channel = channel;
        m_servlet = servlet;
        m_serverConfig = serverConfig;
    }

    public HttpRequest getRequest() {
        return m_request;
    }

    public Channel getChannel() {
        return m_channel;
    }

    public void processRequest() {
        try {
            DefaultHttpServletRequest servletRequest = new DefaultHttpServletRequest(m_request, m_channel);
            DefaultHttpServletResponse servletResponse = new DefaultHttpServletResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK, m_serverConfig.getInitialResponseBufferSize());
            servletResponse.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

            try {
                m_servlet.service(servletRequest, servletResponse);
            } catch (Throwable t) {
                servletResponse.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                LOGGER.error( "Dropping message - " + t.getLocalizedMessage());

            }

            try {
                servletResponse.flushBuffer();
                postResponse(m_channel, m_request, servletResponse.toNettyHttpResponse());

            } catch (IOException e) {
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
