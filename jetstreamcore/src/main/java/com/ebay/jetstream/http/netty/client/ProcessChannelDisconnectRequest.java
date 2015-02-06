/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.channel.Channel;

import com.ebay.jetstream.util.Request;

public class ProcessChannelDisconnectRequest extends Request {

	private HttpClient m_client;
	private Channel m_channelId;
	
	public ProcessChannelDisconnectRequest(HttpClient client, Channel channelId) {
	   m_client = client;
	   m_channelId = channelId;
	   
	}
	@Override
	public boolean execute() {
		
		m_client.processDisconnectedChannel(m_channelId);
		return true;
	}

}
