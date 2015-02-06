/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class InetAddressParserTest {
	
	
	@Test
	public void testValidIpString(){
		InetAddressParser parser = new InetAddressParser("10.4.5.6");
		assertTrue(parser.parse());
	}
	
	@Test
	public void testmatches(){
		InetAddressParser parser = new InetAddressParser("10.5.6.8");
		assertTrue(parser.matches("10.5.6.8"));
	}
	
	@Test
	public void testNotmatches(){
		InetAddressParser parser = new InetAddressParser("10.5.6.8");
		assertFalse(parser.matches("10.5.6.7"));
	}
	

}
