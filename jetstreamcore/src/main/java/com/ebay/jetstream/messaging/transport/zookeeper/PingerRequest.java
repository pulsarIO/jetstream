/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.util.Request;

public class PingerRequest extends Request {
	
	ZooKeeperTransport m_transport ;
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.transport.zookeeper");
	
	
	public PingerRequest(ZooKeeperTransport transport){
		m_transport = transport;
	}

	@Override
	public boolean execute() {
		 try {
			m_transport.checkCnxnState();
		} catch (Exception e) {
			LOGGER.error( "Exception in ZKConnectRequest " , e);
		}
		return true;
	}

}
