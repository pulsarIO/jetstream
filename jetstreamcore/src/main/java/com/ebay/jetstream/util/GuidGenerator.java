/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This will replace the GuidGenerator in future that is already in servicegateway
 * @author jvembunarayanan
 *
 */
public class GuidGenerator {

	private static AtomicLong m_guid;
	private static final long g_prefixMask = 0x7fff000000000000l;
	
	// The purpose of this block is to create a true GUID. The Random
	// object will do a pretty good job of giving us a unique number,
	// but to further permute it, we will mod the current time by a
	// small prime and generate that number of randoms. This can obviously
	// still result in a collision but for a small number of servers
	// the likelihood is extremely small.
	//
	// The random permuation is applied to the upper 15 bits of the
	// long number, insuring the number is positive.
	//

	static {
		m_guid = new AtomicLong(getStartingPrefix() & g_prefixMask);
	}
	
	/**
	 * Increment per call between. Range from
	 * Long.MAX_VALUE to Long.MIN_VALUE, but never
	 * equals 0
	 * @return
	 */
	public static long gen() {
		return m_guid.incrementAndGet();

	} // End of generate

	public static long getPrefix() {
		return m_guid.get() & g_prefixMask;
	}
	
	public static void resetPrefix() {
		long value = m_guid.get();
		long guid = getStartingPrefix();
		m_guid = new AtomicLong((value & g_prefixMask) + guid);
	}
	
	private static long getStartingPrefix() {
		Random rand = new SecureRandom();
		long modIncr = System.currentTimeMillis() % 7;
		long guid = 0;

		for (int c = 0; c <= modIncr; c++) {
			guid = rand.nextLong();
		}

		return guid;
	}
}
