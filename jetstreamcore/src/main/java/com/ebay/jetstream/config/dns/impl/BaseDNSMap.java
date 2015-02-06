/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.config.dns.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.dns.DNSMap;

/**
 * 
 */
public abstract class BaseDNSMap implements DNSMap {
  protected static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.config");
  private static final Pattern s_hostNumber = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");

  private String m_domain;

  protected BaseDNSMap() {
    m_domain = System.getProperty(DOMAINKEY);
  }

  public String fullyQualify(String key) {
    String domain = getDomain();
    if (key.endsWith(".") || domain == null) {
      return key;
    }
    return key + "." + domain + (domain.endsWith(".") ? "" : ".");
  }

  public Iterable<InetAddress> getAllByName(String host) throws UnknownHostException {
    Matcher m = s_hostNumber.matcher(host);
    if (m.matches()) {
      List<InetAddress> result = new ArrayList<InetAddress>();
      result.add(InetAddress.getByName(host));
      return result;
    }
    return null;
  }

  public InetAddress getByName(String host) throws UnknownHostException {
    return s_hostNumber.matcher(host).matches() ? InetAddress.getByName(host) : null;
  }

  public void setDomain(String domain) {
    m_domain = domain;
  }

  protected String getDomain() {
    return m_domain;
  }
}
