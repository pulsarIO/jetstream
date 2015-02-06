/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.dns;

import java.io.InputStream;

import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.dns.impl.DNSFileMap;
import com.ebay.jetstream.config.dns.impl.DNSJNDIMap;

public final class DNSMapFactory {
  private Object m_source = System.getProperty("com.ebay.jetstream.zonefile");

  public String getDnsZoneFile() {
    return m_source instanceof String ? (String) m_source : null;
  }

  public InputStream getDnsZoneStream() {
    return m_source instanceof InputStream ? (InputStream) m_source : null;
  }

  public DNSMap newDNSMap() throws ConfigException {
    DNSMap map = m_source == null ? new DNSJNDIMap() : new DNSFileMap();
    if (m_source != null)
      ((DNSFileMap) map).setSource(m_source);
    return map;
  }

  public void setDnsZoneFile(String theFile) {
    m_source = theFile;
  }

  public void setDnsZoneStream(InputStream theStream) {
    m_source = theStream;
  }
}
