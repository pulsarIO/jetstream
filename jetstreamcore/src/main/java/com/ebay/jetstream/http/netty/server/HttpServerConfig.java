/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.servlet.ServletDefinition;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class HttpServerConfig extends AbstractNamedBean implements XSerializable {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.http.netty.server");
    private long m_rvcBufSz = 8192 * 2;
    private long m_sendBufSz = 8192 * 2;
    private int m_idleTimeoutInSecs = 80000;
    private int m_numIOWorkers = Math.min(2, Runtime.getRuntime().availableProcessors());
    private int m_port = 8084;
    private int m_servletExecutorThreads = Math.min(1, Runtime.getRuntime().availableProcessors());
    private int m_maxWorkQueueSz = 10000;
    private ArrayList<ServletDefinition> m_servletDefinitions = new ArrayList<ServletDefinition>();
    private Map<String, ServletHolder> m_servletInstances = new HashMap<String, ServletHolder>();
    private int m_maxContentLength = 1024 * 1024;
    private int m_initialResponseBufferSize = 1024;
    private int m_maxKeepAliveRequests = -1;
    private int m_maxKeepAliveTimeout = -1;

    /**
	 * 
	 */
    public void destroy() {
        if (!m_servletInstances.isEmpty()) {
            Collection<ServletHolder> servlets = m_servletInstances.values();

            Iterator<ServletHolder> servletItr = servlets.iterator();

            while (servletItr.hasNext()) {
                servletItr.next().getServlet().destroy();
            }
        }
        m_servletDefinitions.clear();
    }

    /**
     * @return the idleTimeoutInSecs
     */
    public int getIdleTimeoutInSecs() {
        return m_idleTimeoutInSecs;
    }

    public int getInitialResponseBufferSize() {
        return m_initialResponseBufferSize;
    }

    /**
     * @return
     */
    public int getMaxContentLength() {
        return m_maxContentLength;
    }

    public int getMaxKeepAliveRequests() {
        return m_maxKeepAliveRequests;
    }

    public int getMaxKeepAliveTimeout() {
        return m_maxKeepAliveTimeout;
    }

    /**
     * @return the maxWorkQureueSz
     */
    public int getMaxWorkQueueSz() {
        return m_maxWorkQueueSz;
    }

    /**
     * @return the numWorkers
     */
    public int getNumIOWorkers() {
        return m_numIOWorkers;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return m_port;
    }

    /**
     * @return the rvcBufSz
     */
    public long getRvcBufSz() {
        return m_rvcBufSz;
    }

    /**
     * @return the sendBufSz
     */
    public long getSendBufSz() {
        return m_sendBufSz;
    }

    /**
     * @return the servletDefinitions
     */
    public ArrayList<ServletDefinition> getServletDefinitions() {
        return m_servletDefinitions;
    }

    /**
     * @return the servletExecutorThreads
     */
    public int getServletExecutorThreads() {
        return m_servletExecutorThreads;
    }

    /**
     * @return the servletInstances
     */

    @Hidden
    public Map<String, ServletHolder> getServletInstances() {
        return m_servletInstances;
    }

    /**
     * @throws InstantiationException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     */
    public void instantiateServlets() throws InstantiationException, ClassNotFoundException, IllegalAccessException {

        for (ServletDefinition servletdef : m_servletDefinitions) {
            HttpServlet servlet;
            try {
                Object obj = servletdef.getServletClass().newInstance();
                servlet = (HttpServlet) obj;
                try {
                    servlet.init();
                } catch (ServletException e) {
                    LOGGER.error( "failed to initialize servlet - " + servletdef.getUrlPath());
                    continue;
                }
                ServletHolder servletHolder = new ServletHolder(servlet);
                m_servletInstances.put(servletdef.getUrlPath(), servletHolder);
            } catch (IllegalAccessException e) {
                throw e;
            } catch (InstantiationException e) {
                LOGGER.error(
                        "failed to intantiate servlet - " + servletdef.getServletClass() + e.getLocalizedMessage());
                throw e;
            }

        }

    }

    /**
     * @param idleTimeoutInSecs
     *            the idleTimeoutInSecs to set
     */
    public void setIdleTimeoutInSecs(int idleTimeoutInSecs) {
        m_idleTimeoutInSecs = idleTimeoutInSecs;
    }

    public void setInitialResponseBufferSize(int initialResponseBufferSize) {
        this.m_initialResponseBufferSize = initialResponseBufferSize;
    }

    /**
     * @param m_maxContentLength
     */
    public void setMaxContentLength(int maxContentLength) {
        this.m_maxContentLength = maxContentLength;
    }

    public void setMaxKeepAliveRequests(int maxKeepAliveRequests) {
        this.m_maxKeepAliveRequests = maxKeepAliveRequests;
    }

    public void setMaxKeepAliveTimeout(int maxKeepAliveTimeout) {
        this.m_maxKeepAliveTimeout = maxKeepAliveTimeout;
    }

    /**
     * @param maxWorkQureueSz
     *            the maxWorkQureueSz to set
     */
    public void setMaxWorkQueueSz(int maxWorkQueueSz) {
        m_maxWorkQueueSz = maxWorkQueueSz;
    }

    /**
     * @param numWorkers
     *            the numWorkers to set
     */
    public void setNumIOWorkers(int numWorkers) {
        m_numIOWorkers = numWorkers;
    }

    /**
     * @param port
     *            the port to set
     */
    public void setPort(int port) {
        m_port = port;
    }

    /**
     * @param rvcBufSz
     *            the rvcBufSz to set
     */
    public void setRvcBufSz(long rvcBufSz) {
        m_rvcBufSz = rvcBufSz;
    }

    /**
     * @param sendBufSz
     *            the sendBufSz to set
     */
    public void setSendBufSz(long sendBufSz) {
        m_sendBufSz = sendBufSz;
    }

    public void setServletDefinitions(ArrayList<ServletDefinition> servletDefinitions) throws InstantiationException,
            ClassNotFoundException, IllegalAccessException {

        m_servletDefinitions.clear();
        m_servletDefinitions.addAll(servletDefinitions);
        instantiateServlets();
    }

    /**
     * @param servletExecutorThreads
     *            the servletExecutorThreads to set
     */
    public void setServletExecutorThreads(int servletExecutorThreads) {
        m_servletExecutorThreads = servletExecutorThreads;
    }
}
