/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MongoScopesUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(MongoScopesUtil.class.getPackage().getName());
	
	public static final boolean isLocalEligible(String changedBeanScope) {
		boolean eligible = false;
		try {
			List<String> servers = parseServerInfo(changedBeanScope);
			String localhostname = java.net.InetAddress.getLocalHost().getHostName();
			if(servers.contains(localhostname)) {
				eligible = true;
			} else {
				localhostname = java.net.InetAddress.getLocalHost().getCanonicalHostName();
				if(servers.contains(localhostname)) {
					eligible = true;
				}
			}
		} catch(Exception e) {
			LOGGER.info( "isLocalEligible method ran into exception ", e);
		}
		
		return eligible;
	}
	
	public static final boolean isDCEligible(String changedBeanScope) {
		boolean eligible = false;
		try {
			List<String> datacenters = parseServerInfo(changedBeanScope);
			String localhostname = java.net.InetAddress.getLocalHost().getCanonicalHostName();
			int index = localhostname.indexOf(".");
			if(index != -1) {
				String domainName = localhostname.substring(index+1);
				if(domainName != null) {
					for(String datacenter : datacenters) {
						if(domainName.contains(datacenter)) {
							eligible = true;
							break;
						}
					}	
				}
			}
			
		} catch(Exception e) {
			LOGGER.info( "isDCEligible method ran into exception ", e);
		}
		
		return eligible;
	}
	
	public static final List<String> parseServerInfo(String changedBeanScope) {
		List<String> servers = new ArrayList<String>();
		try {
			String[] values = StringUtils.split(changedBeanScope, ":");
			if(values.length >= 2) {
				String serverInfo = values[1];
				servers = Arrays.asList(serverInfo.split(","));
				
			} else {
				LOGGER.warn( "parseServerInfo - didn't pass arguments properly, value has to be either local:something or dc:something but it was : "+changedBeanScope);
			}	
		} catch(Exception e) {
			LOGGER.info( "parseServerInfo method ran into exception ", e);
		}
		
		return servers;
	}
	


}
