/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import com.ebay.jetstream.util.Request;

public class ShutdownRequest extends Request {

	private EventProducer m_ep;
	
	public ShutdownRequest(EventProducer ep) {
		m_ep = ep;
	}
	
	@Override
	public boolean execute() {
		m_ep.closeAllConnections();
		return false;
	}

}
