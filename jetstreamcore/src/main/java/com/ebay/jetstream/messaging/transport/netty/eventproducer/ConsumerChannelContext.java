/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;
/*
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
*/

/**
 * @author shmurthy
 * 
 */
public class ConsumerChannelContext implements ChannelHandlerContext, XSerializable {

	private final ConcurrentHashMap<String, Object> m_attributes = new ConcurrentHashMap<String, Object>();
	private Channel m_channel;
	private Object m_attachment;
	private String m_name = "ConsumerChannelContext";
	private String m_remoteAddress;
	private int m_port;
	private NettyVirtualQueueMonitor m_virtualQueueMonitor = new NettyVirtualQueueMonitor(
			3000); // we will pick a max back log of 3K events

	
	public String getRemoteAddress() {
		return m_remoteAddress;
	}

	public void seRemoteAddress(String remoteAddress) {
		this.m_remoteAddress = remoteAddress;
	}
	
	@Override
	public int hashCode() {
		
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((m_attachment == null) ? 0 : m_attachment.hashCode());
		result = prime * result
				+ ((m_attributes == null) ? 0 : m_attributes.hashCode());
		result = prime * result
				+ ((m_channel == null) ? 0 : m_channel.hashCode());
		result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
		result = prime
				* result
				+ ((m_virtualQueueMonitor == null) ? 0 : m_virtualQueueMonitor
						.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ConsumerChannelContext other = (ConsumerChannelContext) obj;
		if (m_attachment == null) {
			if (other.m_attachment != null)
				return false;
		} else if (!m_attachment.equals(other.m_attachment))
			return false;
		if (m_attributes == null) {
			if (other.m_attributes != null)
				return false;
		} else if (!m_attributes.equals(other.m_attributes))
			return false;
		if (m_channel == null) {
			if (other.m_channel != null)
				return false;
		} else if (!m_channel.equals(other.m_channel))
			return false;
		if (m_name == null) {
			if (other.m_name != null)
				return false;
		} else if (!m_name.equals(other.m_name))
			return false;
		if (m_virtualQueueMonitor == null) {
			if (other.m_virtualQueueMonitor != null)
				return false;
		} else if (!m_virtualQueueMonitor.equals(other.m_virtualQueueMonitor))
			return false;
		return true;
	}

	public NettyVirtualQueueMonitor getVirtualQueueMonitor() {
		return m_virtualQueueMonitor;
	}
	
	public void setVirtualQueueMonitor(NettyVirtualQueueMonitor virtualQueueMonitor) {
		m_virtualQueueMonitor = virtualQueueMonitor;
	}

	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getAttachment()
	 */
	
	@Hidden 
	public Object getAttachment() {
		return m_attachment;
	}

	/**
	 * @param attrname
	 * @return
	 */
	@Hidden
	public Object getAttribute(String attrname) {
		return m_attributes.get(attrname);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getChannel()
	 */
	// @Override
	public Channel getChannel() {
		return m_channel;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getHandler()
	 */
	// @Override
	@Hidden
	public ChannelHandler getHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getName()
	 */
	// @Override
	@Hidden
	public String getName() {
		return m_name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getPipeline()
	 */
	// @Override
	@Hidden
	public ChannelPipeline getPipeline() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	public void setAttachment(Object attachment) {
		m_attachment = attachment;

	}
	
	
	/**
	 * @param attrname
	 * @param attrval
	 */
	
	/*
	public void setAttribute(String attrname, Object attrval) {
		m_attributes.put(attrname, attrval);
	}
	*/
	

	/**
	 * @param channel
	 */
	
	public void setChannel(Channel channel) {
		
		m_channel = channel;
		m_remoteAddress = ((InetSocketAddress) channel.remoteAddress()).getAddress().getHostAddress();
		m_port = ((InetSocketAddress) channel.remoteAddress()).getPort();
		
	}

	
	/**
	 * @param name
	 *            the name to set
	 */
	/*
	public void setName(String name) {
		m_name = name;
	}
	*/
	
	@Override
	public <T> Attribute<T> attr(AttributeKey<T> key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel channel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventExecutor executor() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandler handler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isRemoved() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ChannelHandlerContext fireChannelRegistered() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@Deprecated
	public
	ChannelHandlerContext fireChannelUnregistered() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelActive() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelInactive() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireUserEventTriggered(Object event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelRead(Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelReadComplete() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext fireChannelWritabilityChanged() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress,
			SocketAddress localAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture disconnect() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture close() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@Deprecated
	public
	ChannelFuture deregister() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress,
			ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress,
			SocketAddress localAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture disconnect(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture close(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@Deprecated
	public
	ChannelFuture deregister(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelHandlerContext read() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture write(Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture write(Object msg, ChannelPromise promise) {
		return m_channel.writeAndFlush(msg, promise);
		
	}

	@Override
	public ChannelHandlerContext flush() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
		return m_channel.write(msg, promise);
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg) {
		return m_channel.write(msg);
	}

	@Override
	public ChannelPipeline pipeline() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBufAllocator alloc() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPromise newPromise() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelProgressivePromise newProgressivePromise() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture newSucceededFuture() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture newFailedFuture(Throwable cause) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPromise voidPromise() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getPort() {
		return m_port;
	}

	public void setPort(int port) {
		this.m_port = port;
	}
}
