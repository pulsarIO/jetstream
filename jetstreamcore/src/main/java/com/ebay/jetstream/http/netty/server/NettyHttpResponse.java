/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

public class NettyHttpResponse extends DefaultFullHttpResponse {

	public NettyHttpResponse(HttpVersion version, HttpResponseStatus status, ByteBuf buf) {
		super(version, status, buf);
		
	}

}
