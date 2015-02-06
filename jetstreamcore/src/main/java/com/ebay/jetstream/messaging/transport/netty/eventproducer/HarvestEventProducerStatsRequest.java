/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.util.Request;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * worker - gathers event producer stats
 * 
 */

public class HarvestEventProducerStatsRequest extends Request {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	private EventProducer m_eventproducer = null;
	private EventProducerStats m_stats = null;
	
	/**
	 * 
	 */
	public HarvestEventProducerStatsRequest() {}
	
	/**
	 * @param tsm
	 * @param stats
	 */
	public HarvestEventProducerStatsRequest(EventProducer tsm, EventProducerStats stats)
	{
		m_eventproducer = tsm;
		m_stats = stats;
		
	}
	
	/* (non-Javadoc)
	 * @see com.ebay.jetstream.util.Request#execute()
	 */
	@Override
	public boolean execute() {
				
		try {
			m_eventproducer.harvestStats(m_stats);
		} catch (Throwable t) {
			  
            String msg = "Caught exception when executing HarvestEventProducerStatsRequest - " + t.getMessage();
            
            LOGGER.warn( msg);
		}
		return true;
	}
}
