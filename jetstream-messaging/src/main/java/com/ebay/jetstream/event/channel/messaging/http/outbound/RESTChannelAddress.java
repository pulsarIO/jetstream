/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging.http.outbound;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.event.channel.ChannelAddress;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public class RESTChannelAddress extends ChannelAddress implements XSerializable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.channel.messaging.http.outbound");

	private List<String> m_urlList = new ArrayList<String>();
	private List<URI> m_uriList = new ArrayList<URI>();

	public List<String> getUrlList() {
		return Collections.unmodifiableList(m_urlList);
	}

	@Hidden
	public List<URI> getUriList() {
		return Collections.unmodifiableList(m_uriList);
	}
	
	public void setUrlList(List<String> urlList) {
		this.m_urlList = urlList;

		Iterator<String> itr =  urlList.iterator();

		String uriStr = "";
		while(itr.hasNext()) {
			URI uri;
			try {
				uriStr = itr.next();
				uri = new URI(uriStr);
				m_uriList.add(uri);
			} catch (NoSuchElementException e) {
				LOGGER.error( "Failed to create URI for" + uriStr + " : " + e.getLocalizedMessage());
			} catch (URISyntaxException e) {
				LOGGER.error( "Failed to create URI for" + uriStr + " : " + e.getLocalizedMessage());
				
			}
		}
	}
}
