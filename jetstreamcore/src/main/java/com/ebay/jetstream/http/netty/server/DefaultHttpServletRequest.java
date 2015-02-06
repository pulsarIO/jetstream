/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.CookieDecoder;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultHttpServletRequest implements HttpServletRequest {

    private static class IteratorEnumeration<E> implements Enumeration<E> {
        private final Iterator<E> iterator;

        public IteratorEnumeration(Iterator<E> iterator) {
            this.iterator = iterator;
        }

        public E nextElement() {
            return iterator.next();
        }

        public boolean hasMoreElements() {
            return iterator.hasNext();
        }

    }

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.http.netty.server");

    private final HttpRequest m_req;
    QueryStringDecoder m_queryStringDecoder;
    Map<String, List<String>> m_params;
    Channel m_channel;
    JetstreamServletInputStream m_inputStream;

    public DefaultHttpServletRequest(HttpRequest req, Channel channel) {
        m_req = req;
        m_queryStringDecoder = new QueryStringDecoder(req.getUri());
        m_params = m_queryStringDecoder.parameters();
        m_channel = channel;
        m_inputStream = new JetstreamServletInputStream(req);
    }

    @Override
    public Object getAttribute(String arg0) {
        // TODO Auto-generated method stub

        return null;
    }

    @Override
    public Enumeration getAttributeNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getAuthType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCharacterEncoding() {
        // TODO Auto-generated method stub

        return null;
    }

    @Override
    public int getContentLength() {
        return Integer.parseInt(m_req.headers().get(HttpHeaders.Names.CONTENT_LENGTH));
    }

    @Override
    public String getContentType() {
        return m_req.headers().get(HttpHeaders.Names.CONTENT_TYPE);
    }

    @Override
    public String getContextPath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Cookie[] getCookies() {

        String cookieString = getHeader(HttpHeaders.Names.COOKIE);
        if (cookieString != null) {
            Set<io.netty.handler.codec.http.Cookie> cookies = CookieDecoder.decode(cookieString);

            if (!cookies.isEmpty()) {
                Cookie[] cookiecopy = new Cookie[cookies.size()];

                int i = 0;
                for (io.netty.handler.codec.http.Cookie reqcookie : cookies) {
                    cookiecopy[i] = new Cookie(reqcookie.getName(), reqcookie.getValue());
                    cookiecopy[i].setDomain(reqcookie.getDomain());
                    cookiecopy[i].setPath(reqcookie.getPath());
                    cookiecopy[i].setMaxAge((int) reqcookie.getMaxAge());
                    cookiecopy[i].setVersion(reqcookie.getVersion());
                }

                return cookiecopy;
            }
        }

        return null;
    }

    @Override
    public long getDateHeader(String arg0) {
        Date date = new Date(m_req.headers().get(HttpHeaders.Names.DATE));
        return date.getTime();
    }

    @Override
    public String getHeader(String arg0) {
        return m_req.headers().get(arg0);
    }

    @Override
    public Enumeration getHeaderNames() {
        Set<String> headerNames = m_req.headers().names();
        return new IteratorEnumeration<String>(headerNames.iterator());
    }

    @Override
    public Enumeration getHeaders(String arg0) {
        return new IteratorEnumeration<String>(m_req.headers().getAll(arg0).iterator());
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        return m_inputStream;
    }

    @Override
    public int getIntHeader(String arg0) {
        return Integer.parseInt(m_req.headers().get(arg0));
    }

    @Override
    public String getLocalAddr() {
        try {
            return new String(InetAddress.getLocalHost().getAddress());
        } catch (UnknownHostException e) {
            LOGGER.error( e.getMessage(), e);
            return null;
        }
    }

    @Override
    public Locale getLocale() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Enumeration getLocales() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getLocalName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getLocalPort() {
        return URI.create(m_req.getUri()).getPort();
    }

    @Override
    public String getMethod() {
        return m_req.getMethod().name();
    }

    @Override
    public String getParameter(String arg0) {
        return m_params.get(arg0).toString();
    }

    @Override
    public Map getParameterMap() {
        return Collections.unmodifiableMap(m_params);
    }

    @Override
    public Enumeration getParameterNames() {
        return (Enumeration) m_params.keySet();
    }

    @Override
    public String[] getParameterValues(String arg0) {
        List<String> list = m_params.get(arg0);
        return list.toArray(new String[list.size()]);
    }

    @Override
    public String getPathInfo() {
        return URI.create(m_req.getUri()).getPath();
    }

    @Override
    public String getPathTranslated() {
        return URI.create(m_req.getUri()).getPath();
    }

    @Override
    public String getProtocol() {
        return m_req.getProtocolVersion().protocolName();
    }

    @Override
    public String getQueryString() {
        return URI.create(m_req.getUri()).getQuery();
    }

    @Override
    public BufferedReader getReader() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRealPath(String arg0) {
        return URI.create(m_req.getUri()).getRawPath(); // I hope this means raw
                                                        // path - clarify with
                                                        // Tim. R
    }

    @Override
    public String getRemoteAddr() {

        InetSocketAddress addr = (InetSocketAddress) m_channel.remoteAddress();

        return addr.getAddress().getHostAddress();
    }

    @Override
    public String getRemoteHost() {
        InetSocketAddress addr = (InetSocketAddress) m_channel.remoteAddress();
        return addr.getAddress().getHostName();
    }

    @Override
    public int getRemotePort() {
        InetSocketAddress addr = (InetSocketAddress) m_channel.remoteAddress();
        return addr.getPort();
    }

    @Override
    public String getRemoteUser() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRequestedSessionId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRequestURI() {
        return m_req.getUri();
    }

    @Override
    public StringBuffer getRequestURL() {
        try {
            String urlstr = URI.create(m_req.getUri()).toURL().toString();
            StringBuffer buf = new StringBuffer(urlstr);
            return buf;
        } catch (MalformedURLException e) {
            LOGGER.error( e.getMessage(), e);
        }

        return null;
    }

    @Override
    public String getScheme() {

        return URI.create(m_req.getUri()).getScheme();

    }

    @Override
    public String getServerName() {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getServerPort() {

        return getLocalPort();

    }

    @Override
    public String getServletPath() {
        return URI.create(m_req.getUri()).getRawPath(); // needs testing
    }

    @Override
    public HttpSession getSession() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HttpSession getSession(boolean arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Principal getUserPrincipal() {
        return null;
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromUrl() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isRequestedSessionIdValid() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isSecure() {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isUserInRole(String arg0) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void removeAttribute(String arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setAttribute(String arg0, Object arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCharacterEncoding(String arg0) throws UnsupportedEncodingException {
        // TODO Auto-generated method stub

    }

    @Override
    public AsyncContext getAsyncContext() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DispatcherType getDispatcherType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ServletContext getServletContext() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isAsyncStarted() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isAsyncSupported() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public AsyncContext startAsync() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AsyncContext startAsync(ServletRequest request, ServletResponse response) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Part getPart(String name) throws IOException, ServletException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void login(String username, String password) throws ServletException {
        // TODO Auto-generated method stub

    }

    @Override
    public void logout() throws ServletException {
        // TODO Auto-generated method stub

    }

    @Override
    public long getContentLengthLong() {
        return getContentLength();
    }

    @Override
    public String changeSessionId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends HttpUpgradeHandler> T upgrade(Class<T> arg0) throws IOException, ServletException {
        // TODO Auto-generated method stub
        return null;
    }

}
