/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.config;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ebay.jetstream.util.NetMask;

/**
 * 
 */
public class NetworkMap {
  public static List<NetMask> asList(String addresses) throws ConfigException {
    List<NetMask> list = new ArrayList<NetMask>();
    for (String string : addresses.split("[,;\n\r\t]")) {
      string = string.trim();
      if (string.length() > 0)
        list.add(new NetMask(string));
    }
    return list;
  }

  public static String asString(List<NetMask> addresses) {
    StringBuffer buffer = new StringBuffer(addresses.size() * 21);
    for (NetMask address : addresses) {
      if (buffer.length() > 0)
        buffer.append(", ");
      buffer.append(address.getMask());
    }
    return buffer.toString();
  }

  private final Map<String, List<NetMask>> m_addressMap = new HashMap<String, List<NetMask>>();

  /**
   * Gets the first found category of the given address. That is, the first key string for which the address is within
   * the list of network masks.
   * 
   * @param addr
   *            the inet address
   * @return the first assigned category of the address or null if unassigned
   */
  public String getAddressCategory(InetAddress addr) {
    for (String category : getAddressMap().keySet())
      if (isWithinRange(addr, category))
        return category;
    return null;
  }

  public Map<String, List<NetMask>> getAddressMap() {
    return Collections.unmodifiableMap(m_addressMap);
  }

  public List<NetMask> getCategoryRanges(String category) {
    List<NetMask> result = getAddressMap().get(category);
    return result == null ? Collections.<NetMask> emptyList() : result;
  }

  /**
   * Checks whether the given address is within any network mask for the given category.
   * 
   * @param addr
   *            the inet address to check
   * @param category
   *            the category to check
   * @return true iff the given inet address is in the category
   */
  public boolean isWithinRange(InetAddress addr, String category) {
    for (NetMask mask : getCategoryRanges(category))
      if (mask.isWithinMask(addr))
        return true;
    return false;
  }

  public void setAddressMap(Map<String, List<NetMask>> addressMap) {
    m_addressMap.clear();
    m_addressMap.putAll(addressMap);
  }

  public void setCategoryRanges(String category, List<NetMask> addresses) {
    m_addressMap.put(category, addresses);
  }
}
