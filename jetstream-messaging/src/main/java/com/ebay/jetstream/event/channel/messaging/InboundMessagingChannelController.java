/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.channel.messaging.InboundMessagingChannel.Signal;
import com.ebay.jetstream.management.Management;

@ManagedResource(objectName = "Event/ChannelController", description = "Inbound messaging controller")
public class InboundMessagingChannelController extends AbstractNamedBean
    implements InitializingBean {
    
    private InboundMessagingChannel m_channel;
    private final LongCounter m_pauseCount = new LongCounter();
    private final LongCounter m_resumeCount = new LongCounter();
    
    public InboundMessagingChannel getChannel() {
        return m_channel;
    }

    public void setChannel(InboundMessagingChannel channel) {
        this.m_channel = channel;
    }
    
    @ManagedOperation
    public void pause() throws EventException {
        m_pauseCount.increment();
        m_channel.notifyProducer(Signal.PAUSE);
    }

    
    @ManagedOperation
    public void resume() throws EventException {
        m_resumeCount.increment();
        m_channel.notifyProducer(Signal.RESUME);
    }

    /**
     * @return the pauseCount
     */
    public long getTotalPauseCount() {
        return m_pauseCount.get();
    }

    /**
     * @return the resumeCount
     */
    public long getTotalResumeCount() {
        return m_resumeCount.get();
    }

    
    @Override
    public void afterPropertiesSet() throws Exception {
        Management.removeBeanOrFolder(getBeanName(), this);
        Management.addBean(getBeanName(), this);
    }
}
