/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import com.ebay.jetstream.util.Request;

/**
 * @author weifang
 * 
 */
public class ProactiveConnectionCloseRequest extends Request {
	
	private EventProducer m_ep;

	public ProactiveConnectionCloseRequest(EventProducer ep) {
		m_ep = ep;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.util.Request#execute()
	 */
	@Override
	public boolean execute() {
		m_ep.proactiveCloseConnections();
		return true;
	}

}
