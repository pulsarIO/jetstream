/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */
public class HttpClientConfig extends AbstractNamedBean implements XSerializable {
    private int m_maxContentLength = 4 * 1024 * 1024;
    private int m_initialRequestBufferSize = 8192;
    private int numConnections = 1;
    private long m_rvcBufSz = 8192 * 2;
    private long m_sendBufSz = 8192 * 2;
    private int m_connectionTimeoutInSecs = 10;
    private int m_idleTimeoutInSecs = 80000;
    private int m_numWorkers = 1;
    private int m_dispatchThreadPoolSz = 1;
    private int m_dispatchWorkQueueSz = 10000;
    private int m_batchSz = 5;
    private long m_maxNettyBacklog = 3000;
    private String m_compressEncoder = null;
    private int m_maxSessionDurationInSecs = 900; // 15 mins is default
    public HttpClientConfig() {
    }
    public int getBatchSz() {
        return m_batchSz;
    }
    public String getCompressEncoder() {
        return m_compressEncoder;
    }

    /**
     * @return the connectionTimeoutInSecs
     */
    public int getConnectionTimeoutInSecs() {
        return m_connectionTimeoutInSecs;
    }
    
    public int getDispatchThreadPoolSz() {
        return m_dispatchThreadPoolSz;
    }

    public int getDispatchWorkQueueSz() {
        return m_dispatchWorkQueueSz;
    }

    /**
     * @return the idleTimeoutInSecs
     */
    public int getIdleTimeoutInSecs() {
        return m_idleTimeoutInSecs;
    }

    public int getInitialRequestBufferSize() {
        return m_initialRequestBufferSize;
    }

    public int getMaxContentLength() {
        return m_maxContentLength;
    }

    public long getMaxNettyBacklog() {
        return m_maxNettyBacklog;
    }

    public int getMaxSessionDurationInSecs() {
        return m_maxSessionDurationInSecs;
    }

    /**
     * @return the numConnections
     */
    public int getNumConnections() {
        return numConnections;
    }

    /**
     * @return the numWorkers
     */
    public int getNumWorkers() {
        return m_numWorkers;
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

    public void setBatchSz(int batchSz) {
        m_batchSz = batchSz;
    }

    public void setCompressEncoder(String compressEncoder) {
        this.m_compressEncoder = compressEncoder;
    }

    /**
     * @param connectionTimeoutInSecs
     *            the connectionTimeoutInSecs to set
     */
    public void setConnectionTimeoutInSecs(int connectionTimeoutInSecs) {
        m_connectionTimeoutInSecs = connectionTimeoutInSecs;
    }

    public void setDispatchThreadPoolSz(int m_dispatchThreadPoolSz) {
        this.m_dispatchThreadPoolSz = m_dispatchThreadPoolSz;
    }

    public void setDispatchWorkQueueSz(int m_dispatchWorkQueueSz) {
        this.m_dispatchWorkQueueSz = m_dispatchWorkQueueSz;
    }

    /**
     * @param idleTimeoutInSecs
     *            the idleTimeoutInSecs to set
     */
    public void setIdleTimeoutInSecs(int idleTimeoutInSecs) {
        m_idleTimeoutInSecs = idleTimeoutInSecs;
    }

    public void setInitialRequestBufferSize(int initialRequestBufferSize) {
        this.m_initialRequestBufferSize = initialRequestBufferSize;
    }

    public void setMaxContentLength(int maxContentLength) {
        this.m_maxContentLength = maxContentLength;
    }

    public void setMaxNettyBacklog(long maxNettyBacklog) {
        this.m_maxNettyBacklog = maxNettyBacklog;
    }

    public void setMaxSessionDurationInSecs(int maxSessionDurationInSecs) {
        this.m_maxSessionDurationInSecs = maxSessionDurationInSecs;
    }

    /**
     * @param numConnections
     *            the numConnections to set
     */
    public void setNumConnections(int numConnections) {
        this.numConnections = numConnections;
    }

    /**
     * @param numWorkers
     *            the numWorkers to set
     */
    public void setNumWorkers(int numWorkers) {
        this.m_numWorkers = numWorkers;
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

}
