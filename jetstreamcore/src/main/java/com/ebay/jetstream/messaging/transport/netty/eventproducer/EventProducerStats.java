/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.stats.TransportStats;
import com.ebay.jetstream.messaging.transport.netty.registry.EventConsumerAffinityRegistry;
import com.ebay.jetstream.messaging.transport.netty.registry.EventConsumerRegistry;
import com.ebay.jetstream.xmlser.Hidden;

public class EventProducerStats extends TransportStats {

    private long m_totalBytesSent;
    private long m_totalRequestsSent;
    private int m_downStreamQueueBacklog;
    private long m_dropsForMissingAK;
    private long m_dropsForVQOverflow;
    private long m_dropsForNoConsumer;
    private long m_noConsumerAdvisories;
    private long m_vqOverflowAdvisories;
    private long m_missingAKAdvisories;
    private long m_otherMsgDropAdvisories;
    private long m_totalRawBytes;
    private long m_totalCompressedBytes;
    private EventConsumerRegistry m_eventConsumerRegistry; // key
    private EventConsumerAffinityRegistry m_affinityRegistry;
    private ContextConfig m_contextConfig;
    private TransportConfig m_transportConfig;

    /**
   * 
   */
    public EventProducerStats() {
    }

    /**
     * @return the affinityRegistry
     */
    @Hidden
    public EventConsumerAffinityRegistry getAffinityRegistry() {
        return m_affinityRegistry;
    }

    /**
     * @return
     */
    public ContextConfig getContextConfig() {
        return m_contextConfig;
    }

    /**
     * @return the downStreamQueueBacklog
     */
    public int getDownStreamQueueBacklog() {
        return m_downStreamQueueBacklog;
    }

    public long getDropsForMissingAK() {
        return m_dropsForMissingAK;
    }

    public long getDropsForNoConsumer() {
        return m_dropsForNoConsumer;
    }

    public long getDropsForVQOverflow() {
        return m_dropsForVQOverflow;
    }

    /**
     * @return the eventConsumerRegistry
     */
    @Hidden
    public EventConsumerRegistry getEventConsumerRegistry() {
        return m_eventConsumerRegistry;
    }

    public long getMissingAKAdvisories() {
        return m_missingAKAdvisories;
    }

    public long getNoConsumerAdvisories() {
        return m_noConsumerAdvisories;
    }

    public long getOtherMsgDropAdvisories() {
        return m_otherMsgDropAdvisories;
    }

    /**
     * @return the totalBytesSent
     */
    public long getTotalBytesSent() {
        return m_totalBytesSent;
    }

    /**
     * @return compressed bytes after compress
     */
    public long getTotalCompressedBytes() {
        return m_totalCompressedBytes;
    }

    /**
     * @return raw bytes be compressed
     */
    public long getTotalRawBytes() {
        return m_totalRawBytes;
    }

    /**
     * @return the total requests.
     */
    public long getTotalRequestsSent() {
        return m_totalRequestsSent;
    }

    public TransportConfig getTransportConfig() {
        return m_transportConfig;
    }

    public long getVqOverflowAdvisories() {
        return m_vqOverflowAdvisories;
    }

    /**
     * @param affinityRegistry
     *            the affinityRegistry to set
     */
    public void setAffinityRegistry(EventConsumerAffinityRegistry affinityRegistry) {
        m_affinityRegistry = affinityRegistry;
    }

    /**
     * @param contextConfig
     */
    public void setContextConfig(ContextConfig contextConfig) {
        m_contextConfig = contextConfig;
    }

    /**
     * @param downStreamQueueBacklog
     *            the downStreamQueueBacklog to set
     */
    public void setDownStreamQueueBacklog(int downStreamQueueBacklog) {
        m_downStreamQueueBacklog = downStreamQueueBacklog;
    }

    public void setDropsForMissingAK(long dropsForMissingAK) {
        this.m_dropsForMissingAK = dropsForMissingAK;
    }

    public void setDropsForNoConsumer(long dropsForNoConsumer) {
        this.m_dropsForNoConsumer = dropsForNoConsumer;
    }

    public void setDropsForVQOverflow(long dropsForVQOverflow) {
        this.m_dropsForVQOverflow = dropsForVQOverflow;
    }

    /**
     * @param eventConsumerRegistry
     *            the eventConsumerRegistry to set
     */
    public void setEventConsumerRegistry(EventConsumerRegistry eventConsumerRegistry) {
        m_eventConsumerRegistry = eventConsumerRegistry;
    }

    public void setMissingAKAdvisories(long missingAKAdvisories) {
        this.m_missingAKAdvisories = missingAKAdvisories;
    }

    public void setNoConsumerAdvisories(long noConsumerAdvisories) {
        this.m_noConsumerAdvisories = noConsumerAdvisories;
    }

    public void setOtherMsgDropAdvisories(long otherMsgDropAdvisories) {
        this.m_otherMsgDropAdvisories = otherMsgDropAdvisories;
    }

    /**
     * @param totalBytesSent
     */
    public void setTotalBytesSent(long totalBytesSent) {

        m_totalBytesSent = totalBytesSent;
    }

    public void setTotalCompressedBytes(long totalCompressedBytes) {
        this.m_totalCompressedBytes = totalCompressedBytes;
    }

    public void setTotalRawBytes(long totalRawBytes) {
        this.m_totalRawBytes = totalRawBytes;
    }

    public void setTotalRequestsSent(long totalRequestsSent) {
        this.m_totalRequestsSent = totalRequestsSent;
    }

    public void setTransportConfig(TransportConfig transportConfig) {
        this.m_transportConfig = transportConfig;
    }

    public void setVqOverflowAdvisories(long vqOverflowAdvisories) {
        this.m_vqOverflowAdvisories = vqOverflowAdvisories;
    }

}
