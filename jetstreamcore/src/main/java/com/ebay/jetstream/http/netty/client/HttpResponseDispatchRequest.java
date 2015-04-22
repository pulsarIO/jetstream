/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class HttpResponseDispatchRequest implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.http.netty.client");
	private ResponseFuture m_future;
	private HttpResponse m_response;
	
	public HttpResponseDispatchRequest(ResponseFuture future, HttpResponse response) {
	
		m_future = future;
		m_response = response;
		
	}
	
	@Override
	public void run() {
		
		try {
			m_future.operationComplete(m_response);
		} catch (Throwable t) {
			LOGGER.error( "Error while dispatching response" + t.getLocalizedMessage());
		}
		finally {
			 ReferenceCountUtil.release(m_response);
		}
	}

}
