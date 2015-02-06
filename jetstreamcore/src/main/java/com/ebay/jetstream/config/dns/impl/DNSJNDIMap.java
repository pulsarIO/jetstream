/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.dns.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.NotFoundException;
import com.ebay.jetstream.config.NotInitializedException;
import com.ebay.jetstream.config.dns.DNSServiceRecord;
import com.ebay.jetstream.util.CommonUtils;

public class DNSJNDIMap extends BaseDNSMap {
  public static final String DNSSERVER = "com.ebay.jetstream.dnsserver";

  private static final String s_TXT[] = { "TXT" };
  private static final String s_SRV[] = { "SRV" };
  private static final String s_A[] = { "A" };
  private static final String s_PTR[] = { "PTR" };

  private final String m_server;
  private DirContext m_context;

  public DNSJNDIMap() throws ConfigException {
    m_server = System.getProperty(DNSSERVER);

    StringBuilder sb = new StringBuilder();
    sb.append("dns://");
    if (m_server != null) {
      sb.append(m_server);
    }
    sb.append("/");

    String providerURL = sb.toString();

    Hashtable<String, String> env = new Hashtable<String, String>();
    env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
    env.put("java.naming.provider.url", providerURL);

    try {
      String info = "java.naming.provider.url=" + providerURL + "&Status=DirContextInited";
      LOGGER.info(info);
      m_context = new InitialDirContext(env);
    }
    catch (Throwable e) {
      LOGGER.error( "Unable to initialize DNS context", e);
      LOGGER.error( "java.naming.provider.url=" + providerURL + "&ERRORCODE=UnableToInitializeDNSCtx"
          + "&ERROR=" + e.getMessage() + CommonUtils.redirectPrintStackTraceToString(e));
      throw new ConfigException("Unable to initialize DNS context", e);
    }
  }

  private void checkInitialized() throws NotInitializedException {
    if (m_context == null)
      throw new NotInitializedException();
  }

  @Override
  public Iterable<InetAddress> getAllByName(String host) throws UnknownHostException {
    String qhost = fullyQualify(host);
    Iterable<InetAddress> all = super.getAllByName(qhost);
    if (all != null)
      return all;

    List<InetAddress> result = new ArrayList<InetAddress>();
    try {
      Attributes attrs = m_context.getAttributes(qhost, s_A);
      Attribute a = attrs.getAll().next();
      NamingEnumeration<?> vals = a.getAll();
      while (vals.hasMore()) {
        result.add(InetAddress.getByName((String) vals.next()));
      }
      if (result.size() > 0) {
        return result;
      }
    }
    catch (NamingException ne) {
      // Rely on generic DNS.
    }

    for (InetAddress a : InetAddress.getAllByName(qhost)) {
      result.add(a);
    }

    return result;
  }

  @Override
  public InetAddress getByName(String host) throws UnknownHostException {
    String qhost = fullyQualify(host);
    InetAddress result = super.getByName(qhost);
    if (result != null)
      return result;

    try {
      Attributes attrs = m_context.getAttributes(qhost, s_A);
      return InetAddress.getByName((String) attrs.getAll().next().get());
    }
    catch (NamingException ne) {
      // We'll rely on generic DNS.
    }

    return InetAddress.getByName(qhost);
  }

  public List<DNSServiceRecord> getMultiSRV(String key) throws ConfigException {
    checkInitialized();
    String qkey = fullyQualify(key);

    List<DNSServiceRecord> result = new ArrayList<DNSServiceRecord>();
    try {
      Attributes attrs = m_context.getAttributes(qkey, s_SRV);
      Attribute a = attrs.getAll().next();
      NamingEnumeration<?> vals = a.getAll();
      while (vals.hasMore()) {
        result.add(new DNSServiceRecord((String) vals.next()));
      }
    }
    catch (Throwable e) {
      LOGGER.debug(getClass().getName(), "getMultiSRV", e);
      throw new NotFoundException("Unable to retrieve key: " + qkey, e);
    }

    return result;
  }

  public String getPTR(InetAddress inetAddress) throws ConfigException {
    checkInitialized();

    String ip = inetAddress.getHostAddress();

    // reverse it
    String[] ipArray = ip.split("\\.");
    int len = ipArray.length;
    StringBuilder strBuilder = new StringBuilder();
    for (int i = len - 1; i >= 0; i--) {
      strBuilder.append(ipArray[i] + ".");
    }

    String key = strBuilder.toString() + "in-addr.arpa";
    try {
      Attributes attrs = m_context.getAttributes(key, s_PTR);
      String result = (String) attrs.getAll().next().get();
      return result;
    }
    catch (Throwable e) {
      LOGGER.debug(getClass().getName(), "getTXT", e);
      // throw new NotFoundException("Unable to resolve FQDN for: " + ip, e);
    }
    return key;
  }

  public DNSServiceRecord getSRV(String key) throws ConfigException {
    checkInitialized();
    String qkey = fullyQualify(key);

    try {
      Attributes attrs = m_context.getAttributes(qkey, s_SRV);
      return new DNSServiceRecord((String) attrs.getAll().next().get());
    }
    catch (Throwable e) {
      LOGGER.debug(getClass().getName(), "getSRV", e);
      // throw new NotFoundException("Unable to retrieve key: " + qkey, e);
    }
    return null;
  }

  public String getTXT(String key) throws ConfigException {
    checkInitialized();

    String qkey = fullyQualify(key);
    try {
      Attributes attrs = m_context.getAttributes(qkey, s_TXT);
      return (String) attrs.getAll().next().get();
    }
    catch (Throwable e) {
      LOGGER.debug(getClass().getName(), "getTXT", e);
      // throw new NotFoundException("Unable to retrieve key: " + qkey, e);
    }
    return null;
  }

}
