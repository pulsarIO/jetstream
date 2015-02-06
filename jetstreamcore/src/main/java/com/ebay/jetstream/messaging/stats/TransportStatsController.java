/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.stats;

import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.messaging.interfaces.ITransportProvider;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy
 *
 * Transport management and monitoring
 */
@ManagedResource
public class TransportStatsController  implements XSerializable {

	private ITransportProvider m_provider;
	
	/**
	 * 
	 */
	public TransportStatsController() {}
	
	/**
	 * @param provider
	 */
	public TransportStatsController(ITransportProvider provider)
	{
	
		m_provider = provider;
	}
	
	public TransportStats getStats()
	{
		return m_provider.getStats();
	}
	
	@ManagedOperation
	public void resetStats()
	{
		m_provider.resetStats();
	}
}
