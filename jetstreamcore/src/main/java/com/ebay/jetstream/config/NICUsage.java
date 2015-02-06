/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.dns.DNSMap;
import com.ebay.jetstream.util.NetMask;

public class NICUsage {
  public interface AddressFilter {
    boolean isIn(InetAddress addr);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger("com.jetstream.config");

  private DNSMap m_dnsMap;
  private final Map<String, NetMask> m_dnsTypeMasks = new HashMap<String, NetMask>();
  private final List<NetworkInterfaceInfo> m_niis = new ArrayList<NetworkInterfaceInfo>();

  public NICUsage() throws ConfigException {
    try {
      // For all nics, add a NetworkInterfaceInfo
      for (NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces()))
        m_niis.add(new NetworkInterfaceInfo(nic));
    }
    catch (SocketException e) {
      throw new ConfigException("Initialization failed", e);
    }
  }

  public void addNetMask(String serviceType, NetMask mask) {
	  m_dnsTypeMasks.put(serviceType, mask);
  }
  
  public List<InetAddress> getAllInetAddresses(AddressFilter filter) {
    List<InetAddress> addrs = new ArrayList<InetAddress>();
    for (NetworkInterfaceInfo nii : m_niis) {
      for (Enumeration<InetAddress> ii = nii.getNIC().getInetAddresses(); ii.hasMoreElements();) {
        InetAddress addr = ii.nextElement();
        if (filter == null || filter.isIn(addr))
          addrs.add(addr);
      }
    }
    return addrs;
  }

  public DNSMap getDnsMap() {
    return m_dnsMap;
  }

  public List<InetAddress> getFilteredInetAddressList(String type, AddressFilter filter) {
    List<InetAddress> addrs = new ArrayList<InetAddress>();
    for (NetworkInterfaceInfo nii : m_niis) {
      List<InetAddress> typeAddrs = nii.getAddressesForType(type);
      for (InetAddress addr : typeAddrs)
        if (filter == null || filter.isIn(addr))
          addrs.add(addr);
    }
    return addrs;
  }

  public List<InetAddress> getInetAddressListByUsage(String type) {
    List<InetAddress> addrs = new ArrayList<InetAddress>();
    for (NetworkInterfaceInfo nii : m_niis) {
      addrs.addAll(nii.getAddressesForType(type));
    }
    return addrs;
  }

  public List<NetworkInterfaceInfo> getNICUsageList() {
    return Collections.unmodifiableList(m_niis);
  }

  public void registerDnsAssignedType(String dnsType) throws ConfigException {
    if (!m_dnsTypeMasks.containsKey(dnsType))
      try {
        String txt = m_dnsMap.getTXT(dnsType);
        m_dnsTypeMasks.put(dnsType, new NetMask(txt));
        LOGGER.info(dnsType + " NetMask=" + txt);
        addNICInfo(dnsType);
      }
      catch (NotFoundException e) {
        String warning = "Failed to load NetMask for " + dnsType + ".  Error Message \""
            + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()) + "\"";
        LOGGER.warn(warning);
      }
  }

  public void setDnsMap(DNSMap dnsMap) {
    m_dnsMap = dnsMap;
  }

  public void addNICInfo(String dnsType) {
    for (NetworkInterfaceInfo nii : m_niis) {
      for (InetAddress addr : Collections.list(nii.getNIC().getInetAddresses())) {
        NetMask mask = m_dnsTypeMasks.get(dnsType);
        if (mask.isWithinMask(addr)) {
          nii.addAddressForType(dnsType, addr);
        }
      } // for addrs
    } // for nics
  }
 
}