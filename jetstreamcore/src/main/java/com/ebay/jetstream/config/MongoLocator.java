/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.dns.DNSMap;
import com.ebay.jetstream.config.dns.DNSMapFactory;

public final class MongoLocator {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MongoLocator.class.getPackage().getName());

	public static String getMongoLocation() {
		String mongoLocationFromDNS = null;
		
		NICUsage nicusage = null;
		DNSMapFactory dnsfactory = null;

		try {
			nicusage = new NICUsage();
		} catch (ConfigException e) {
			LOGGER.error( e.getMessage(), e);
		}

		dnsfactory = new DNSMapFactory();
		String zonefileloc = ConfigUtils.getPropOrEnv("MONGOLOCATIONZONEFILE"); //KEEPME
        
        // if zone file is supplied we will use it else we fall back to JNDI
        if (zonefileloc != null) {
              dnsfactory.setDnsZoneFile(zonefileloc);
        }


		try {
			if (nicusage != null) {
				nicusage.setDnsMap(dnsfactory.newDNSMap());
			}
		} catch (ConfigException e) {
			LOGGER.error( e.getMessage(), e);
		}

		SelfLocator selflocator = new SelfLocator();

		try {
			selflocator.setNicUsage(nicusage);

		} catch (ConfigException e) {
			LOGGER.error( e.getMessage(), e);
		}

		try {
			if (nicusage != null) {
				DNSMap map = nicusage.getDnsMap();
				if (map != null) {
					mongoLocationFromDNS = map.getTXT("jetstream_mongo_config_url");
				}
			}
		} catch(Exception e) {
			LOGGER.error( e.getMessage(), e);
		} 
		
		
		LOGGER.info( " Mongo location from DNS is : " +mongoLocationFromDNS);
		
		return mongoLocationFromDNS;
	}
	
}
