/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Meta Data Object describing the Network Interface
 *
 * @author jianwu
 * @since 08/18/2006
 */
public final class NetworkInterfaceInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	private transient NetworkInterface m_nic; 
	private Map<String, List<InetAddress>> m_assignedTypes = new HashMap<String, List<InetAddress>>();

	public NetworkInterfaceInfo(NetworkInterface nic) {
		m_nic = nic;
	}

	/**
	 * @return the NIC
	 */
	public NetworkInterface getNIC() {
		return m_nic;
	}

	/**
	 * @return the types and addresses assigned to this NIC
	 */
	public Map<String, List<InetAddress>> getAssignedTypes() {
		return Collections.unmodifiableMap(m_assignedTypes);
	}

	public List<InetAddress> getAddressesForType(String type) {
		List<InetAddress> addrs = m_assignedTypes.get(type);
		return addrs == null ? Collections.<InetAddress>emptyList() : Collections.unmodifiableList(addrs);
	}

	
	public void addAddressForType(String type, InetAddress addr) {
		List<InetAddress> addrs = m_assignedTypes.get(type);
		if (addrs == null)
			m_assignedTypes.put(type, addrs = new ArrayList<InetAddress>());
		if (!addrs.contains(addr))
			addrs.add(addr);
	}

	public String summary() {
		StringBuilder sb = new StringBuilder("NIC \"").append(m_nic.getName()).append("\", assigned types (");
		for (Map.Entry<String, List<InetAddress>> entry : m_assignedTypes.entrySet()) {
			sb.append(" ").append(entry.getKey()).append(" uses addresses: (");
			for (InetAddress addr : entry.getValue()) {
				sb.append(" ").append(addr.getHostAddress());
			}
			sb.append(")");
		}
		sb.append(")");
		return sb.toString();
	}
}
