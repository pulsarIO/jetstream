/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import io.netty.handler.codec.http.HttpRequest;

import java.net.URI;

import com.ebay.jetstream.messaging.transport.netty.eventproducer.NettyVirtualQueueMonitor;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class HttpSessionChannelFuture extends DefaultChannelPromise implements XSerializable {
	private HttpRequest m_message;
	private final NettyVirtualQueueMonitor m_virtualQueueMonitor;
	private int m_ttl = 4; // default ttl is 4 - means 4 retries
	private URI m_uri;
	

	public URI getUri() {
		return m_uri;
	}

	public void setUri(URI uri) {
		this.m_uri = uri;
	}

	public int getTtl() {
		return m_ttl;
	}

	public void setTtl(int ttl) {
		this.m_ttl = ttl;
	}

	public int decrementAndGetTTL() {
		if (m_ttl > 0)
			m_ttl -= 1;
		return m_ttl;
	}

	public NettyVirtualQueueMonitor getVirtualQueueMonitor() {
		return m_virtualQueueMonitor;
	}

	/**
	 * @param channel
	 */
	protected HttpSessionChannelFuture(Channel channel,
			NettyVirtualQueueMonitor virtualQueueMonitor) {
		 super(channel);
		m_virtualQueueMonitor = virtualQueueMonitor;

	}

	/**
	 * @return the message
	 */
	public HttpRequest getMessage() {
		return m_message;
	}

	/**
	 * @param message
	 *            the message to set
	 */
	public void setMessage(HttpRequest message) {
		m_message = message;
	}
}
