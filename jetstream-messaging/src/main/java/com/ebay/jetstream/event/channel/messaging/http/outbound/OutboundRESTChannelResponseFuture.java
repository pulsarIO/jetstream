/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging.http.outbound;


import io.netty.handler.codec.http.HttpResponse;

import com.ebay.jetstream.http.netty.client.AbstractResponseFuture;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class OutboundRESTChannelResponseFuture extends AbstractResponseFuture {

  private OutboundRESTChannel m_channel;
  
  public OutboundRESTChannelResponseFuture(OutboundRESTChannel channel) {
		m_channel = channel;
		
  }
	
  @Override
	public void operationComplete(HttpResponse response) {

		if (isFailure())
			m_channel.incrementDropCounter();
	
		if (isSuccess())
			m_channel.incrementSendCounter();
	}

}
