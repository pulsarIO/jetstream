/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.util.Request;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * This is a request object corresponding to an advertisement. It is a work
 * item for processing EventConsumerAdvertisements
 */

public class EventConsumerAdvertisementRequest extends Request {
	
    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
  
    private EventProducer m_tsm = null;
	private EventConsumerAdvertisement m_eca = null;
        
	/**
	 * @param tsm
	 * @param eca
	 */
	public EventConsumerAdvertisementRequest(EventProducer tsm, EventConsumerAdvertisement eca)
	{
		m_tsm = tsm;
		m_eca = eca;
	}
	
	/* (non-Javadoc)
	 * @see com.ebay.jetstream.util.Request#execute()
	 */
	@Override
	public boolean execute() {
			
		try {
			m_tsm.processEventConsumerAdvertisement(m_eca);
		} catch (Throwable t) {
			
            String msg = "Failed to Process Event Consumer Advertisement - " + t.getMessage();
            LOGGER.warn( msg);
		}
		return true;
	}

}
