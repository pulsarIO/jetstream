/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.messaging.transport.netty.eventproducer;



import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

/**
 * @author shmurthy@ebay.com
 * 
 */

public class EventConsumerChannelFuture extends ExtendedChannelPromise {
	
	private JetstreamMessage message;
	private NettyVirtualQueueMonitor m_virtualQueueMonitor;
	private boolean m_compressionEnabled;
	private ConsumerChannelContext m_consumerChannelContext;

    public ConsumerChannelContext getConsumerChannelContext() {
		return m_consumerChannelContext;
	}

	public void setconsumerChannelContext(
			ConsumerChannelContext consumerChannelContext) {
		this.m_consumerChannelContext = consumerChannelContext;
	}

	public boolean isCompressionEnabled() {
		return m_compressionEnabled;
	}

	public void setCompressionEnabled(boolean compressionEnabled) {
		this.m_compressionEnabled = compressionEnabled;
	}

	public NettyVirtualQueueMonitor getVirtualQueueMonitor() {
		return m_virtualQueueMonitor;
	}

	public void setVirtualQueueMonitor(
			NettyVirtualQueueMonitor m_virtualQueueMonitor) {
		this.m_virtualQueueMonitor = m_virtualQueueMonitor;
	}

	/**
	 * @param channel
	 */
	protected EventConsumerChannelFuture(Channel channel, NettyVirtualQueueMonitor virtualQueueMonitor) {
		super(channel);
		m_virtualQueueMonitor = virtualQueueMonitor;
	}

	
	public void addListener(ChannelFutureListener listener) {
		super.addListener(listener);
	}

	
	/**
	 * @return the message
	 */
	public JetstreamMessage getMessage() {
		return message;
	}

	
	
	/**
	 * @param message
	 *          the message to set
	 */
	public void setMessage(JetstreamMessage message) {
		this.message = message;
	}
}
