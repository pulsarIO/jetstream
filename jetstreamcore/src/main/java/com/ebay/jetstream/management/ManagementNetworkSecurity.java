/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management;

import java.net.InetAddress;

import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.NetworkMap;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * 
 */
public class ManagementNetworkSecurity implements XSerializable {
  private static ManagementNetworkSecurity s_instance;

  public static ManagementNetworkSecurity getInstance() {
    return s_instance;
  }

  private final NetworkMap m_networkMap = new NetworkMap();

  // TODO: This should be looked at. Currently, if this constructor is never called,
  // security is disabled. Need to make this safer by changing how static is set.
  // Perhaps a static activate() method would be preferable.
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public ManagementNetworkSecurity() {
    s_instance = this;
  }

  public String getReadOnlyAddresses() {
    return NetworkMap.asString(m_networkMap.getCategoryRanges("RO"));
  }

  public String getReadWriteAddresses() {
    return NetworkMap.asString(m_networkMap.getCategoryRanges("RW"));
  }

  public boolean isAuthorized(InetAddress addr, boolean forWrite) {
    return m_networkMap.isWithinRange(addr, "RW") || !forWrite && m_networkMap.isWithinRange(addr, "RO");
  }

  public void setReadOnlyAddresses(String addresses) throws ConfigException {
    m_networkMap.setCategoryRanges("RO", NetworkMap.asList(addresses));
  }

  public void setReadWriteAddresses(String addresses) throws ConfigException {
    m_networkMap.setCategoryRanges("RW", NetworkMap.asList(addresses));
  }
}
