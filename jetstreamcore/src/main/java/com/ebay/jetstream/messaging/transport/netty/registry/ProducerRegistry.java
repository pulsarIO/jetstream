/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.registry;


import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
// import org.jboss.netty.channel.Channel;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This registry holds information about all producers connecting to a consumer node. It maintains a binding of remote host-port to
 *          Netty channel.
 * 
 */

public class ProducerRegistry {

	private static final long serialVersionUID = 1L;

	private ConcurrentHashMap<String, Channel> m_epr = new ConcurrentHashMap<String, Channel>(); // key
																									// =
																									// hostAndPort

	/**
	 * @param channel
	 */
	public void add(Channel channel) {
		m_epr.put(getHostAndPort(channel), channel);
	}

	/**
	 * @param channel
	 */
	public void remove(Channel channel) {
		m_epr.remove(getHostAndPort(channel));
	}

	/**
	 * @return
	 */
	public Enumeration<Channel> getAllProducers() {
		return m_epr.elements();
	}

	/**
	 * @param channel
	 * @return
	 */
	private String getHostAndPort(Channel channel) {
		StringBuffer buf = new StringBuffer();

		buf.append(((InetSocketAddress) channel.remoteAddress())
				.getAddress().getHostAddress());
		buf.append("-");
		buf.append(((InetSocketAddress) channel.remoteAddress()).getPort());
		return buf.toString();
	}
}
