/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs.util;

import java.net.InetAddress;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author weifang
 * 
 */
public class MiscUtil {
	private static final Logger LOGGER = Logger.getLogger(MiscUtil.class
			.getName());

	public static boolean objEquals(Object o1, Object o2) {
		if (o1 == null) {
			return o2 == null;
		} else {
			return o1.equals(o2);
		}
	}

	private static volatile String localHostName;

	public static String getLocalHostName() {
		if (localHostName == null) {
			synchronized (MiscUtil.class) {
				if (localHostName == null) {
					try {
						InetAddress s = InetAddress.getLocalHost();
						localHostName = s.getCanonicalHostName();
					} catch (Throwable e) {
						LOGGER.log(Level.SEVERE, "Can resolve local host name",
								e);
					}
				}
			}
		}

		return localHostName;
	}

	public static String getUniqueNameWithNumbers(Collection<String> names,
			String baseName) {
		if (names == null) {
			return baseName;
		}
		String name = baseName;
		int i = 1;
		while (names.contains(name)) {
			name = baseName + i;
			i++;
		}
		return name;
	}

	public static long maxToZero(long input) {
		if (input == Long.MAX_VALUE) {
			return 0;
		} else {
			return input;
		}
	}
}
