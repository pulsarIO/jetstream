/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventconsumer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.common.NameableThreadFactory;
import com.ebay.jetstream.messaging.transport.netty.compression.MessageDecompressionHandler;
import com.ebay.jetstream.messaging.transport.netty.config.NettyTransportConfig;
import com.ebay.jetstream.messaging.transport.netty.serializer.KryoObjectEncoder;
import com.ebay.jetstream.messaging.transport.netty.serializer.NettyObjectEncoder;
import com.ebay.jetstream.messaging.transport.netty.serializer.StreamMessageDecoder;




/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * Acceptor binds to the specified port and establishes a channel pipe line factory to create new channles on the consumer side. 
 * This class creates new Channels every time a new connection request from a producer is received.
 */

public class Acceptor {

	public static final int OBJ_ENCODER_BUFLEN = 3000;
	private SocketAddress m_acceptorSocket;
	EventLoopGroup m_bossGroup; 
	EventLoopGroup m_workerGroup;

	private String m_ipAddress = "";
	private int m_tcpPort = -1;
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.transport.netty");
	private int m_numIoProcessors;
	ServerBootstrap m_serverbootstrap;
	private long m_readIdleTimeout = 24 * 3600;
	private boolean m_enableCompression = false;
	private boolean m_tcpKeepAlive = false;
	private EventProducerSessionHandler m_rxSessionHandler;
	private NettyTransportConfig m_tc;
	


	public boolean isTcpKeepAlive() {
		return m_tcpKeepAlive;
	}


	public void setTcpKeepAlive(boolean tcpKeepAlive) {
		this.m_tcpKeepAlive = tcpKeepAlive;
	}


	/**
	 * @param numIoProcessors
	 */
	public Acceptor(NettyTransportConfig tc) {
		m_tc = tc;
		m_numIoProcessors = tc.getNumConnectorIoProcessors();
	}


	/**
	 * @return
	 */
	public boolean isEnableCompression() {
		return m_enableCompression;
	}

	/**
	 * @param enableCompression
	 */
	public void setEnableCompression(boolean enableCompression) {
		this.m_enableCompression = enableCompression;
	}


	/**
	 * @param rxSessionHandler
	 * @throws Exception
	 */
	public void bind(EventProducerSessionHandler rxSessionHandler)
			throws Exception {

		m_rxSessionHandler = rxSessionHandler;
		m_bossGroup = new NioEventLoopGroup(1, new NameableThreadFactory("NettyAcceptor-" + m_tc.getTransportName()));
		m_workerGroup = new NioEventLoopGroup(m_numIoProcessors, new NameableThreadFactory("NettyReceiver-" + m_tc.getTransportName()));

		try {
			m_serverbootstrap = new ServerBootstrap(); 
			m_serverbootstrap.group(m_bossGroup, m_workerGroup)
			.channel(NioServerSocketChannel.class) 
			.childHandler(new ChannelInitializer<SocketChannel>() { 
				@Override
				public void initChannel(SocketChannel ch) throws Exception {

					ch.pipeline().addFirst("msghandler", m_rxSessionHandler);

					StreamMessageDecoder decoder = new StreamMessageDecoder(ClassResolvers
                            .weakCachingConcurrentResolver(null));
                    ch.pipeline().addBefore(
                            "msghandler",
                            "msgdecoder",
                            decoder);
                    ch.pipeline().addBefore("msgdecoder", "kryoEcnoder", new KryoObjectEncoder());
                    ch.pipeline().addBefore("kryoEcnoder", "msgencoder", new NettyObjectEncoder());

					if (m_enableCompression) {

						ch.pipeline().addBefore("msgencoder", "decompressor", new MessageDecompressionHandler(false, 250000));

						ch.pipeline().addBefore("decompressor", "idleTimeoutHandler", new IdleStateHandler((int) m_readIdleTimeout, 0, 0));
					}
					else
						ch.pipeline().addBefore("msgencoder", "idleTimeoutHandler", new IdleStateHandler((int) m_readIdleTimeout, 0, 0));


				}
			})
			.option(ChannelOption.SO_BACKLOG, 128)
			.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
			.childOption(ChannelOption.TCP_NODELAY, true)
			.childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
			.childOption(ChannelOption.SO_KEEPALIVE, isTcpKeepAlive());
			
			
			if (m_tc.getReceivebuffersize() > 0) {
			    m_serverbootstrap.childOption(ChannelOption.SO_RCVBUF, m_tc.getReceivebuffersize());
			}
			
			if (m_tc.getSendbuffersize() > 0) {
			    m_serverbootstrap.childOption(ChannelOption.SO_SNDBUF, m_tc.getSendbuffersize());
			}

		} catch (Exception t) {
			throw t;
		}

		// we are given a port from DNS. We will check if it is taken. If it is
		// we will increment the port # by 10 and keep trying 10 times.
		// adding this feature so that we can operate the cluster on a single
		// node. Very useful for debugging and also for demos.

		int retryCount = 20;
		int port = m_tcpPort;

		while (retryCount-- > 0) {
			if (isPortAvailable(port))
				break;
			port += 1;

		}

		if (retryCount == 0)
			return; // we did not find a port to bind

		m_tcpPort = port;

		LOGGER.info( "Acceptor bound to port" + m_tcpPort);

	}

	/**
	 * @return the ipAddress
	 */
	public String getIpAddress() {
		return m_ipAddress;
	}

	/**
	 * @return the numIoProcessors
	 */
	public int getNumIoProcessors() {
		return m_numIoProcessors;
	}

	/**
	 * @return the readIdleTimeout
	 */
	public long getReadIdleTimeout() {
		return m_readIdleTimeout;
	}

	/**
	 * @return the tcpPort
	 */
	public int getTcpPort() {
		return m_tcpPort;
	}

	/**
	 * @param ipAddress
	 *            the ipAddress to set
	 */
	public void setIpAddress(String ipAddress) {
		m_ipAddress = ipAddress;
	}

	/**
	 * @param numIoProcessors
	 *            the numIoProcessors to set
	 */
	public void setNumIoProcessors(int numIoProcessors) {
		m_numIoProcessors = numIoProcessors;
	}

	/**
	 * @param readIdleTimeout
	 *            the readIdleTimeout to set
	 */
	public void setReadIdleTimeout(long readIdleTimeout) {
		m_readIdleTimeout = readIdleTimeout;
	}

	/**
	 * @param tcpPort
	 *            the tcpPort to set
	 */
	public void setTcpPort(int tcpPort) {
		m_tcpPort = tcpPort;
	}

	/**
	 * 
	 */

	public void unbind() {

		m_bossGroup.shutdownGracefully();
		m_workerGroup.shutdownGracefully();
		
	}

	/**
	 * @param port
	 * @return
	 */
	private boolean isPortAvailable(int port) {

		try {
			m_acceptorSocket = new InetSocketAddress(m_ipAddress, port);

			ChannelFuture f = m_serverbootstrap.bind(m_acceptorSocket).sync(); 

			return true;

		} catch (Exception e) {

		}

		return false;
	}
}