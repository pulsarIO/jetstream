/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.CookieDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.ServerCookieEncoder;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

public class DefaultHttpServletResponse implements HttpServletResponse {

    private DefaultFullHttpResponse m_nettyhttpresp;
    private ByteBufOutputStream m_bufStream;
    private PrintWriter m_writer;
    private ByteBuf m_buffer;
    private int m_bufsz = 1024;
    private JetstreamServletOutputStream m_servletOutputStream;

    public DefaultHttpServletResponse(HttpVersion version, HttpResponseStatus status, int bufsz) {
        m_bufsz = bufsz;
        if (m_bufsz == 0) {
            m_buffer = Unpooled.EMPTY_BUFFER;
        } else {
            m_buffer = Unpooled.buffer(m_bufsz);
        }
        m_nettyhttpresp = new DefaultFullHttpResponse(version, status, m_buffer);
    }

    private void initOutputstream() {
        if (m_writer == null) {
            m_bufStream = new ByteBufOutputStream(m_buffer);
            m_writer = new PrintWriter(m_bufStream);
            m_servletOutputStream = new JetstreamServletOutputStream(m_bufStream);
        }
    }
    
    @Override
    public void addCookie(Cookie arg0) {
        String cookieString = m_nettyhttpresp.headers().get(HttpHeaders.Names.COOKIE);
        Set<io.netty.handler.codec.http.Cookie> cookies;
        if (cookieString != null) {
            cookies = CookieDecoder.decode(cookieString);
            cookies.add(new io.netty.handler.codec.http.DefaultCookie(arg0.getName(), arg0.getValue()));
        } else {
            cookies = new HashSet<io.netty.handler.codec.http.Cookie>();
            cookies.add(new io.netty.handler.codec.http.DefaultCookie(arg0.getName(), arg0.getValue()));
        }

        if (!cookies.isEmpty()) {
            // Reset the cookies if necessary.
            HttpHeaders.setHeader(m_nettyhttpresp, HttpHeaders.Names.SET_COOKIE, ServerCookieEncoder.encode(cookies));
        }
    }

    @Override
    public void addDateHeader(String arg0, long arg1) {
        HttpHeaders.addDateHeader(m_nettyhttpresp, arg0, new Date(arg1));
    }

    @Override
    public void addHeader(String arg0, String arg1) {
        HttpHeaders.addHeader(m_nettyhttpresp, arg0, arg1);

    }

    @Override
    public void addIntHeader(String arg0, int arg1) {
        HttpHeaders.addIntHeader(m_nettyhttpresp, arg0, Integer.valueOf(arg1));
    }

    @Override
    public boolean containsHeader(String arg0) {
        return m_nettyhttpresp.headers().contains(arg0);

    }

    @Override
    public String encodeRedirectUrl(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String encodeRedirectURL(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String encodeUrl(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String encodeURL(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void flushBuffer() throws IOException {
        if (m_writer != null) {
            m_writer.flush();
        }
    }

    @Override
    public int getBufferSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getCharacterEncoding() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getContentType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Locale getLocale() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        initOutputstream();
        return m_servletOutputStream;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        initOutputstream();
        return m_writer;
    }

    @Override
    public boolean isCommitted() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public void resetBuffer() {

    }

    @Override
    public void sendError(int arg0) throws IOException {
        HttpResponseStatus status = new HttpResponseStatus(arg0, null);
        m_nettyhttpresp.setStatus(status);

    }

    @Override
    public void sendError(int arg0, String arg1) throws IOException {
        HttpResponseStatus status = new HttpResponseStatus(arg0, arg1);
        m_nettyhttpresp.setStatus(status);

    }

    @Override
    public void sendRedirect(String arg0) throws IOException {
        // not implemented in netty defaulthttpresponse - TODO later

    }

    @Override
    public void setBufferSize(int arg0) {
        m_bufsz = arg0;
        m_buffer.ensureWritable(arg0);
    }

    @Override
    public void setCharacterEncoding(String arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setContentLength(int arg0) {
        // need to verify if this is true for get
        HttpHeaders.setContentLength(m_nettyhttpresp, arg0);
    }

    @Override
    public void setContentType(String arg0) {
        HttpHeaders.setHeader(m_nettyhttpresp, CONTENT_TYPE, arg0);

    }

    @Override
    public void setDateHeader(String arg0, long arg1) {
        HttpHeaders.setDateHeader(m_nettyhttpresp, arg0, new Date(arg1));

    }

    @Override
    public void setHeader(String arg0, String arg1) {
        HttpHeaders.setHeader(m_nettyhttpresp, arg0, arg1);
    }

    @Override
    public void setIntHeader(String arg0, int arg1) {
        HttpHeaders.setIntHeader(m_nettyhttpresp, arg0, Integer.valueOf(arg1));
    }

    @Override
    public void setLocale(Locale arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStatus(int code) {
        HttpResponseStatus status = HttpResponseStatus.valueOf(code);
        m_nettyhttpresp.setStatus(status);
    }

    @Override
    public void setStatus(int code, String reasonPhrase) {
        HttpResponseStatus status = new HttpResponseStatus(code, reasonPhrase);
        m_nettyhttpresp.setStatus(status);
    }

    public void setStatus(HttpResponseStatus status) {
        m_nettyhttpresp.setStatus(status);
    }

    public DefaultFullHttpResponse toNettyHttpResponse() {
        return m_nettyhttpresp;
    }

    public String getHeader(String name) {
        return m_nettyhttpresp.headers().get(name);
    }

    public Collection<String> getHeaderNames() {
        return m_nettyhttpresp.headers().names();
    }

    public Collection<String> getHeaders(String name) {
        return m_nettyhttpresp.headers().getAll(name);
    }

    public int getStatus() {
        return m_nettyhttpresp.getStatus().code();
    }

    @Override
    public void setContentLengthLong(long arg0) {
        HttpHeaders.setContentLength(m_nettyhttpresp, arg0);
    }

}
