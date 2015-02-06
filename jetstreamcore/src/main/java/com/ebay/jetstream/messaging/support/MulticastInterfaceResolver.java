/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.support;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.NICUsage.AddressFilter;

public class MulticastInterfaceResolver implements AddressFilter {
	private int m_port = 43444; // use some random port
	private MulticastSocket m_socket = null;
	private static final Logger s_LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.support");

	/**
	 * @return
	 */
	private boolean createSocket() {
		try {
			m_socket = new MulticastSocket(m_port);
			m_socket.setLoopbackMode(false);
		} catch (IOException e) {
			s_LOGGER.error(e.getMessage(), e);

			return false;
		}

		return true;
	}

	/**
	 * @param nic
	 * @return
	 */
	private boolean supportsMulticast(NetworkInterface nic) {

		try {

			m_socket.setNetworkInterface(nic);

			return true;

		} catch (SocketException e1) {
		}

		return false;

	}

	/**
	 * 
	 */
	public MulticastInterfaceResolver() {
	}

	/* (non-Javadoc)
	 * @see com.ebay.jetstream.config.NICUsage.AddressFilter#isIn(java.net.InetAddress)
	 */
	public boolean isIn(InetAddress addr) {
		if (createSocket()) {
			Enumeration<NetworkInterface> e;

			try {
				e = NetworkInterface.getNetworkInterfaces();

				while (e.hasMoreElements()) {
					NetworkInterface nic = e.nextElement();

					Enumeration<InetAddress> addrs = nic.getInetAddresses();

					while (addrs.hasMoreElements()) {
						InetAddress inetaddr = addrs.nextElement();

						if (addr.equals(inetaddr)) {
							return supportsMulticast(nic);

						}
					}

				}

			} catch (SocketException e1) {
			}

		} // end of while loop

		return false;

	}

}
