/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.common.NameableThreadFactory;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

public class Acceptor {
    private SocketAddress m_acceptorSocket;
    private SocketAddress m_localAcceptorSocket;
    EventLoopGroup m_bossGroup;
    EventLoopGroup m_workerGroup;
    ServerBootstrap m_serverbootstrap;
    private InetAddress m_ipAddress;
    private int m_tcpPort = -1;
    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
    private int m_numIoProcessors;
    private long m_readIdleTimeout = 24 * 3600;
    private final int m_maxContentLength;
    private final int m_maxKeepAliveRequests;
    private final int m_maxKeepAliveTimeout;
    private final HttpServerConfig m_config;
    
    public Acceptor(HttpServerConfig config) {
        m_numIoProcessors = config.getNumIOWorkers();
        m_maxContentLength = config.getMaxContentLength();
        m_maxKeepAliveRequests = config.getMaxKeepAliveRequests();
        m_maxKeepAliveTimeout = config.getMaxKeepAliveTimeout();
        m_config = config;
    }

    /**
     * @param rxSessionHandler
     * @throws Exception
     */
    public void bind(final HttpRequestHandler httpReqHandler, final HttpServerStatisticsHandler statisticsHandler) throws Exception {
        m_bossGroup = new NioEventLoopGroup(1, new NameableThreadFactory("NettyHttpServerAcceptor"));
        m_workerGroup = new NioEventLoopGroup(m_numIoProcessors, new NameableThreadFactory("NettyHttpServerWorker"));

        try {
            m_serverbootstrap = new ServerBootstrap();
            m_serverbootstrap.group(m_bossGroup, m_workerGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast("timeout", new IdleStateHandler((int) m_readIdleTimeout, 0, 0));
                            ch.pipeline().addLast("stats", statisticsHandler);
                            ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                            ch.pipeline().addLast("encoder", new HttpResponseEncoder());
                            ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                            ch.pipeline().addLast("deflater", new HttpContentCompressor(1));
                            ch.pipeline().addLast("aggregator", new HttpObjectAggregator(m_maxContentLength));
                            if (m_maxKeepAliveRequests > 0 || m_maxKeepAliveTimeout > 0) {
                                ch.pipeline().addLast("keepAliveHandler", new KeepAliveHandler(m_maxKeepAliveRequests, m_maxKeepAliveTimeout));
                            }
                            ch.pipeline().addLast("handler", httpReqHandler);

                        }

                    }).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, false);

            if (m_config.getRvcBufSz() > 0) {
                m_serverbootstrap.childOption(ChannelOption.SO_RCVBUF, (int) m_config.getRvcBufSz());
            }
            
            if (m_config.getSendBufSz() > 0) {
                m_serverbootstrap.childOption(ChannelOption.SO_SNDBUF, (int) m_config.getSendBufSz());
            }
            
        } catch (Exception t) {
            throw t;
        }

        m_acceptorSocket = new InetSocketAddress(m_ipAddress, m_tcpPort);
        m_serverbootstrap.bind(m_acceptorSocket).sync();
        
        if (!m_ipAddress.isLoopbackAddress() && !m_ipAddress.isAnyLocalAddress()) { 
            // Add lookback if not bind
            m_localAcceptorSocket = new InetSocketAddress("127.0.0.1", m_tcpPort);
            m_serverbootstrap.bind(m_localAcceptorSocket).sync();
        }
    }

    /**
     * @return the ipAddress
     */
    public String getIpAddress() {
        return m_ipAddress.toString();
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
    public void setIpAddress(InetAddress ipAddress) {
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

    public void shutDown() {
        m_bossGroup.shutdownGracefully();
        m_workerGroup.shutdownGracefully();

        LOGGER.warn( "shutting down Acceptor");

    }

}
