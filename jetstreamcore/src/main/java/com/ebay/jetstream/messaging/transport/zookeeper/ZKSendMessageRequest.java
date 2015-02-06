/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.util.Request;

public class ZKSendMessageRequest extends Request {
	ZooKeeperTransport m_transport;
	JetstreamMessage m_msg ;
	
	public ZKSendMessageRequest(ZooKeeperTransport transport, JetstreamMessage msg ) {
		m_transport = transport;
		m_msg = msg ;
		
	}
	
	@Override
	public boolean execute() {
			m_transport.setData(m_msg);
			return true;
			
	}

}
