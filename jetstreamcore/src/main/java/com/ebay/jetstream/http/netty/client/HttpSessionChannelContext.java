/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.channel.Channel;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.messaging.transport.netty.eventproducer.NettyVirtualQueueMonitor;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class HttpSessionChannelContext {

    private Channel m_channel;
    private String m_name = "ConsumerChannelContext";
    private final AtomicBoolean m_channelConnected = new AtomicBoolean(false);
    private String m_uri;
    private NettyVirtualQueueMonitor m_virtualQueueMonitor = new NettyVirtualQueueMonitor(60);
    // we will pick a max back log of 30K events
    private AtomicLong m_creationTime = new AtomicLong(System.currentTimeMillis());
    private int m_sessionDurationInSecs;
    private Random m_r = new SecureRandom();

    public HttpSessionChannelContext() {
        m_sessionDurationInSecs = 800 + m_r.nextInt(400);
        // min is 800 secs and we will add an offset to upto 400 so all
        // connections don't close at the same time
    }

    public int getSessionDurationInSecs() {
        return m_sessionDurationInSecs;
    }

    public void setSessionDurationInSecs(int m_sessionDurationInSecs) {
        this.m_sessionDurationInSecs = m_sessionDurationInSecs + m_r.nextInt(400);
    }

    public NettyVirtualQueueMonitor getVirtualQueueMonitor() {
        return m_virtualQueueMonitor;
    }

    public void setVirtualQueueMonitor(NettyVirtualQueueMonitor virtualQueueMonitor) {
        this.m_virtualQueueMonitor = virtualQueueMonitor;
    }

    public String getUri() {
        return m_uri;
    }

    /**
     * @param channelConnected
     *            the channelConnected to set
     */
    public void channelConnected() {
        m_channelConnected.set(true);
    }

    public void channeldisConnected() {
        m_channelConnected.set(false);
    }

    public Channel getChannel() {
        return m_channel;
    }

    public String getName() {
        return m_name;
    }

    /**
     * @return the channelConnected
     */
    public boolean isChannelConnected() {
        return m_channelConnected.get();
    }

    public void setChannel(Channel channel) {
        m_channel = channel;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name) {
        m_name = name;
    }

    public void setUri(String uri) {
        m_uri = uri;
    }

    public boolean isTimeToClose() {
        if ((System.currentTimeMillis() - m_creationTime.get()) >= (m_sessionDurationInSecs * 1000))
            return true;
        else
            return false;
    }

    public boolean isQueueEmpty() {
        return m_virtualQueueMonitor.isEmpty();
    }

}
