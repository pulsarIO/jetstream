/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging.http.outbound;

import java.net.URI;

import com.ebay.jetstream.event.JetstreamEvent;

public class SendRequest {

	private JetstreamEvent m_event;
	private OutboundRESTChannelResponseFuture m_future;
	private URI m_uri;
	

	public URI getUri() {
		return m_uri;
	}

	public void setUri(URI m_uri) {
		this.m_uri = m_uri;
	}

	public JetstreamEvent getEvent() {
		return m_event;
	}
	
	public void setEvent(JetstreamEvent m_event) {
		this.m_event = m_event;
	}
	
	public OutboundRESTChannelResponseFuture getFuture() {
		return m_future;
	}
	
	public void setFuture(OutboundRESTChannelResponseFuture m_future) {
		this.m_future = m_future;
	}
	
}
