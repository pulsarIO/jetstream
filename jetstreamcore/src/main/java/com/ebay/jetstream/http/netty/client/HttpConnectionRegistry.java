/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.channel.Channel;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class HttpConnectionRegistry {
    private final ConcurrentMap<Channel, HttpSessionChannelContext> m_sessions = new ConcurrentHashMap<Channel, HttpSessionChannelContext>();
    private final CopyOnWriteArrayList<Channel> m_connectionIds = new CopyOnWriteArrayList<Channel>();
    private final CopyOnWriteArrayList<HttpSessionChannelContext> m_tobeDiscardedSessions = new CopyOnWriteArrayList<HttpSessionChannelContext>();

    private int m_nextSessionNdx = 0;
    private int m_maxConnections;
    private URI m_uri;

    public URI getUri() {
        return m_uri;
    }

    public int getMaxConnections() {
        return m_maxConnections;
    }

    public void setMaxConnections(int m_maxConnections) {
        this.m_maxConnections = m_maxConnections;
    }

    public void add(HttpSessionChannelContext channelcontext) {
        m_sessions.put(channelcontext.getChannel(), channelcontext);
        m_connectionIds.add(channelcontext.getChannel());
    }

    public void clear() {
        m_sessions.clear();
        m_connectionIds.clear();
    }

    public HttpSessionChannelContext get(Channel channelid) {

        return m_sessions.get(channelid);
    }

    public Collection<HttpSessionChannelContext> getAllSessions() {
        return m_sessions.values();
    }

    public HttpSessionChannelContext getNextSession() {
        if (m_connectionIds.isEmpty())
            return null;
        else {

            if (m_nextSessionNdx > m_connectionIds.size() - 1)
                m_nextSessionNdx = 0;

            HttpSessionChannelContext ctx = m_sessions.get(m_connectionIds.get(m_nextSessionNdx));

            // instead of using a background timer we will use the calling
            // thread to check if it is time to close
            // a connection. we will pick one connection and close it.

            if (ctx.isTimeToClose()) {
                m_tobeDiscardedSessions.add(remove(m_connectionIds.get(m_nextSessionNdx)));
            }

            destroyDiscardedSessions();
            // connection is only destroyed if it has no pending messages.

            if (m_nextSessionNdx > m_connectionIds.size() - 1)
                m_nextSessionNdx = 0;

            try {
            	ctx = m_sessions.get(m_connectionIds.get(m_nextSessionNdx++));
            } catch (Throwable t) {
            	ctx = null;
            }
            
            return ctx;
        }
    }

    public int getSessionCount() {
        return m_sessions.size();
    }

    public boolean isEmpty() {
        return m_sessions.isEmpty();
    }

    public HttpSessionChannelContext remove(Channel channelid) {
        m_connectionIds.remove(channelid);
        return m_sessions.remove(channelid);
    }

    public void setUri(URI uri) {
        m_uri = uri;

    }

    private void destroyDiscardedSessions() {
        List<HttpSessionChannelContext> sessions = new ArrayList<HttpSessionChannelContext>();

        for (HttpSessionChannelContext discardedSession : m_tobeDiscardedSessions) {
            if (discardedSession.isChannelConnected() && discardedSession.isQueueEmpty()) {
                discardedSession.getChannel().close();
                sessions.add(discardedSession);
            }
        }

        for (HttpSessionChannelContext destroyedSessions : sessions) {
            m_tobeDiscardedSessions.remove(destroyedSessions);
        }
    }

}
