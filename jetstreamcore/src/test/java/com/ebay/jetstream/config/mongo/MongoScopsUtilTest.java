/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;

import org.junit.Test;

public class MongoScopsUtilTest {

	@Test
	public void testLocalEligible() throws UnknownHostException{
		
		String localhostname = java.net.InetAddress.getLocalHost().getHostName();
		String scope = "local:"+ localhostname;
		assertTrue(MongoScopesUtil.isLocalEligible(scope));
		
		String canhostname = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		String hostscope = "local:"+ canhostname;
		assertTrue(MongoScopesUtil.isLocalEligible(hostscope));
		
		String scope1= "local:127.0.0.1";
		assertFalse(MongoScopesUtil.isLocalEligible(scope1));
		
		String scope2= "local";
		assertFalse(MongoScopesUtil.isLocalEligible(scope2));
		
		String scope3= "local::";
		assertFalse(MongoScopesUtil.isLocalEligible(scope3));
		
	}
	
	@Test
	public void testDCEligible() throws UnknownHostException{
		String localhostname = java.net.InetAddress.getLocalHost().getHostName();
		String scope = "dc:"+ localhostname;
		assertFalse(MongoScopesUtil.isDCEligible(scope));
		
	}
	
}
