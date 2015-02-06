/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;

import com.ebay.jetstream.xmlser.XSerializable;


/**
 * THis class holds ZooKeeper Server and Port
 * @author rmuthupandian
 *
 */
public class ZooKeeperNode implements XSerializable  {
	
	private String hostname =  "127.0.0.1";
	private int port = 2181;
	
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((hostname == null) ? 0 : hostname.hashCode());
		result = prime * result + port;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ZooKeeperNode other = (ZooKeeperNode) obj;
		if (hostname == null) {
			if (other.hostname != null)
				return false;
		} else if (!hostname.equals(other.hostname))
			return false;
		if (port != other.port)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "ZooKeeperNode [hostname=" + hostname + ", port=" + port + "]";
	}

}
