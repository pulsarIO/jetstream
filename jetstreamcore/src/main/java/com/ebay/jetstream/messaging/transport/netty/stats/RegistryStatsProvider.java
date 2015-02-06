/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.messaging.transport.netty.stats;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.messaging.transport.netty.NettyTransport;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventProducerStats;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy
 * 
 */
@ManagedResource
public class RegistryStatsProvider implements XSerializable {

  NettyTransport m_transportProvider;

  public RegistryStatsProvider() {
  }

  public RegistryStatsProvider(NettyTransport transportProvider) {

    m_transportProvider = transportProvider;

  }

  public RegistryStats getStats() {
	  
    RegistryStats registrystats = new RegistryStats();

    EventProducerStats ts = (EventProducerStats) m_transportProvider.getStats();
    registrystats.setAffinityRegistry(ts.getAffinityRegistry());
    registrystats.setEventConsumerRegistry(ts.getEventConsumerRegistry());
    registrystats.setTransportConfig(ts.getTransportConfig());

    return registrystats;
  }
}
