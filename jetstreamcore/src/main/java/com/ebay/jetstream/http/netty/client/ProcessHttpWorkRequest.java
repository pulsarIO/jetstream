/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.handler.codec.http.HttpRequest;

import java.net.URI;

import com.ebay.jetstream.util.Request;
/*
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequest;
*/

public class ProcessHttpWorkRequest extends Request {

	private HttpClient m_client;
	private URI m_uri;
	private HttpRequest m_request;
	
	public ProcessHttpWorkRequest(HttpClient client, URI uri, HttpRequest request) {
		m_client = client;
		m_uri = uri;
		m_request = request;
	}
	
	
	@Override
	public boolean execute() {
		m_client.writeRequest(m_uri, m_request);
		return true;
	}

}
