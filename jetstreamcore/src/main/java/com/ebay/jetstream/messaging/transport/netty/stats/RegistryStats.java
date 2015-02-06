/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.messaging.transport.netty.stats;

import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.transport.netty.registry.EventConsumerAffinityRegistry;
import com.ebay.jetstream.messaging.transport.netty.registry.EventConsumerRegistry;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy
 * 
 */
public class RegistryStats implements XSerializable {

  private EventConsumerRegistry m_eventConsumerRegistry; // key
  private EventConsumerAffinityRegistry m_affinityRegistry;
  private TransportConfig m_transportConfig;

  public TransportConfig getTransportConfig() {
	return m_transportConfig;
  }

  public void setTransportConfig(TransportConfig transportConfig) {
	this.m_transportConfig = transportConfig;
}


/**
   * @return the affinityRegistry
   */
  public EventConsumerAffinityRegistry getAffinityRegistry() {
    return m_affinityRegistry;
  }

  /**
   * @return the eventConsumerRegistry
   */
  public EventConsumerRegistry getEventConsumerRegistry() {
    return m_eventConsumerRegistry;
  }

  /**
   * @param affinityRegistry
   *          the affinityRegistry to set
   */
  public void setAffinityRegistry(EventConsumerAffinityRegistry affinityRegistry) {
    m_affinityRegistry = affinityRegistry;
  }

  /**
   * @param eventConsumerRegistry
   *          the eventConsumerRegistry to set
   */
  public void setEventConsumerRegistry(EventConsumerRegistry eventConsumerRegistry) {
    m_eventConsumerRegistry = eventConsumerRegistry;
  }

}
