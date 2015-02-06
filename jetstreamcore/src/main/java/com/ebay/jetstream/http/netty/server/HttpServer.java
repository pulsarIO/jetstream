/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.net.InetAddress;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.http.HttpServlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.ebay.jetstream.util.RequestQueueProcessor;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;
/*
 import org.jboss.netty.channel.Channel;
 import org.jboss.netty.handler.codec.http.HttpRequest;
 import org.jboss.netty.handler.codec.http.HttpResponseStatus;

 */

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

@SuppressWarnings("rawtypes")
@ManagedResource(objectName = "Http/Server", description = "Http server component")
public class HttpServer extends AbstractNamedBean implements XSerializable, BeanChangeAware, InitializingBean,
        ApplicationListener, ShutDownable {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.rtbd.http.netty.server");
    private Map<String, ServletHolder> m_servlets = new ConcurrentHashMap<String, ServletHolder>();
    private HttpRequestHandler m_httpHandler;
    private HttpServerStatisticsHandler m_statisticsHandler;
    private Acceptor m_acceptor;
    private HttpServerConfig m_serverConfig = new HttpServerConfig();
    private RequestQueueProcessor processor;
    private final AtomicBoolean m_shutdownStatus = new AtomicBoolean(false);
    private LongCounter tooBusyCounter = new LongCounter();
    private LongCounter notFoundCounter = new LongCounter();
    private LongCounter invalidCounter = new LongCounter();
    
    public void add(String path, HttpServlet servlet) {
        m_servlets.put(path, new ServletHolder(servlet));
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        m_servlets = getServerConfig().getServletInstances();
        start();

        Management.removeBeanOrFolder(getBeanName(), this);
        Management.addBean(getBeanName(), this);
    }

    @Override
    @Hidden
    public int getPendingEvents() {

        return 0;
    }

    /**
     * @return the requestBacklog
     */
    public long getRequestBacklog() {
        return processor.getPendingRequests();

    }

    /**
     * @return the exceptional request counter
     */
    public long getExceptionRequests() {
        return processor.getDroppedRequests();
    }

    /**
     * @return counter for path not found
     */
    public long getNotFoundRequests() {
        return notFoundCounter.get();
    }
    
    /**
     * @return dropped request counter due to too busy
     */
    public long getDropedRequests() {
        return tooBusyCounter.get();
    }
    
    /**
     * @return invalid request counter
     */
    public long getInvalidRequests() {
        return invalidCounter.get();
    }
    
    /**
     * @return the serverConfig
     */
    public HttpServerConfig getServerConfig() {
        return m_serverConfig;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextBeanChangedEvent) {

            ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

            // Calculate changes
            if (bcInfo.isChangedBean(getServerConfig())) {

                LOGGER.info( "Received new configuration for InboundMessageChannel - " + getBeanName());

                try {
                    shutDown();
                } catch (Throwable e) {
                    // swallow the exception and log
                    String errMsg = "Error closing InboundMessagingChannel while applying new config - ";
                    errMsg += e.getMessage();

                    LOGGER.error( errMsg);

                }
                if (m_serverConfig != null)
                    m_serverConfig.destroy();

                setServerConfig((HttpServerConfig) bcInfo.getChangedBean());

                try {
                    m_servlets.putAll(getServerConfig().getServletInstances());
                    start();
                } catch (Throwable e) {
                    // swallow the exception and log
                    String errMsg = "Error opening InboundMessagingChannel while applying new config - ";
                    errMsg += e.getMessage();

                    LOGGER.error( errMsg);

                }
            }
        }

    }

    public void processHttpRequest(HttpRequest request, Channel channel) throws Exception {

        String path = URI.create(request.getUri()).getPath();

        ServletHolder sh = null;

        if (path == null) {
            invalidCounter.increment();
            new HttpErrorRequest(request, channel, HttpResponseStatus.INTERNAL_SERVER_ERROR).run();
            return;
        }

        sh = m_servlets.get(path);

        if (sh == null) {
            String p = path;

            while (p.lastIndexOf("/") > 0) {
                p = p.substring(0, p.lastIndexOf('/'));
                sh = m_servlets.get(p);
                if (sh != null) {
                    break;
                }
            }
        }

        if (sh == null) {
            notFoundCounter.increment();
            new HttpErrorRequest(request, channel, HttpResponseStatus.NOT_FOUND).run();
            return;
        }

        HttpServlet servlet = (HttpServlet) sh.getServlet();

        if (servlet != null) {
            if (!processor.processRequest(new HttpWorkRequest(request, channel, servlet, m_serverConfig))) {
                tooBusyCounter.increment();
                new HttpErrorRequest(request, channel, HttpResponseStatus.SERVICE_UNAVAILABLE).run();
            }
        } else {
            notFoundCounter.increment();
            new HttpErrorRequest(request, channel, HttpResponseStatus.NOT_FOUND).run();
        }

    }

    public void remove(String path) {
        m_servlets.remove(path);
    }

    public void setServerConfig(HttpServerConfig config) {
        m_serverConfig = config;
    }

    @Override
    public void shutDown() {
        if (m_shutdownStatus.compareAndSet(false, true)) {
            m_acceptor.shutDown();
            processor.shutdown();
            LOGGER.warn("current request backlog =" + getRequestBacklog() + getBeanName());
        }
        Management.removeBeanOrFolder(getBeanName(), this);
    }

    public void start() throws Exception {

        m_httpHandler = new HttpRequestHandler(this);
        m_statisticsHandler = new HttpServerStatisticsHandler();
        m_acceptor = new Acceptor(getServerConfig());
        m_acceptor.setIpAddress(InetAddress.getLocalHost()); 
        m_acceptor.setReadIdleTimeout(getServerConfig().getIdleTimeoutInSecs() * 1000);
        m_acceptor.setTcpPort(getServerConfig().getPort());
        m_acceptor.bind(m_httpHandler, m_statisticsHandler);
        processor = new RequestQueueProcessor(getServerConfig().getMaxWorkQueueSz(), getServerConfig().getServletExecutorThreads(), getBeanName()); 
    }
    
    /**
     * @return the getTotalRcvCount
     */
    public long getTotalBytesRcvd() {
        if (m_statisticsHandler != null) {
            return m_statisticsHandler.getBytesRead();
        }
        return 0;
    }

    
    /**
     * @return the getTotalRcvCount
     */
    public long getRcvCountPerSec() {
        if (m_httpHandler != null) {
            return m_httpHandler.getRcvCountPerSec();
        }
        return 0;
    }

    /**
     * @return the getTotalRcvCount
     */
    public long getTotalRcvCount() {
        if (m_httpHandler != null) {
            return m_httpHandler.getTotalRcvCount();
        }
        return 0;
    }

    /**
     * @return the getTotalContentLength
     */
    public long getTotalContentLength() {
        if (m_httpHandler != null) {
            return m_httpHandler.getTotalContentLength();
        }
        return 0;
    }
    
}
