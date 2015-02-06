/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import com.ebay.jetstream.util.Request;
// import org.jboss.netty.handler.codec.http.DefaultHttpRequest;

public class ShutdownRequest extends Request {

	private HttpClient m_client;
	
	
	public ShutdownRequest(HttpClient client) {
		m_client = client;
		
	}
	
	
	@Override
	public boolean execute() {
		m_client.releaseAllResources();
		return false;
	}
}
