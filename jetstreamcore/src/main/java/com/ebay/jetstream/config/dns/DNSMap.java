/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.dns;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import com.ebay.jetstream.config.ConfigException;


public interface DNSMap {
	String DOMAINKEY = "com.ebay.jetstream.configdomain";
	void setDomain(String domain); 
	String fullyQualify(String host);
	Iterable<InetAddress> getAllByName(String host) throws UnknownHostException;
	InetAddress getByName(String host) throws UnknownHostException;
	List<DNSServiceRecord> getMultiSRV(String key) throws ConfigException;
	DNSServiceRecord getSRV(String key) throws ConfigException;
	String getTXT(String key) throws ConfigException;
	String getPTR(InetAddress inetAddress) throws ConfigException;
}