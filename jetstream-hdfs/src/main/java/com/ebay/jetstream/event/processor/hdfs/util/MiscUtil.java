/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.util;

import java.net.InetAddress;
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
}
