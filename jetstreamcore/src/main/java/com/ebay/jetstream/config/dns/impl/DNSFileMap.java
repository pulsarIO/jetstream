/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.dns.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.xbill.DNS.Master;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.TXTRecord;
import org.xbill.DNS.Type;

import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.config.NotFoundException;
import com.ebay.jetstream.config.dns.DNSServiceRecord;

public class DNSFileMap extends BaseDNSMap {
  private Map<String, List<Record>> m_records;
  private Object m_source;

  @Override
  public Iterable<InetAddress> getAllByName(String host) throws UnknownHostException {
    String qhost = fullyQualify(host);
    Iterable<InetAddress> all = super.getAllByName(qhost);
    if (all != null)
      return all;

    List<InetAddress> result = resolve(makeHostKey(qhost));
    if (result.size() == 0)
      for (InetAddress a : InetAddress.getAllByName(qhost))
        result.add(a);

    return result;
  }

  @Override
  public InetAddress getByName(String host) throws UnknownHostException {
    String qhost = fullyQualify(host);
    InetAddress result = super.getByName(qhost);
    if (result != null)
      return result;

    String hkey = makeHostKey(qhost);
    List<InetAddress> results = resolve(hkey);
    return results.size() == 0 ? InetAddress.getByName(hkey) : results.get(0);
  }

  public List<DNSServiceRecord> getMultiSRV(String key) throws ConfigException {
    String qkey = fullyQualify(key);
    List<DNSServiceRecord> result = new ArrayList<DNSServiceRecord>();

    List<Record> list = m_records.get(makeHostKey(qkey));
    if (list == null) {
      throw new NotFoundException("No such record: " + makeHostKey(qkey));
    }

    for (Record r : list) {
      if (r.getType() != Type.SRV) {
        continue;
      }

      result.add(new DNSServiceRecord(((SRVRecord) r).rdataToString()));
    }

    return result;
  }

  public String getPTR(InetAddress inetAddress) throws ConfigException {
    return inetAddress.getCanonicalHostName();
  }

  public Object getSource() {
    return m_source;
  }

  public DNSServiceRecord getSRV(String key) throws ConfigException {
    String qkey = fullyQualify(key);
    List<Record> list = m_records.get(makeHostKey(qkey));
    if (list == null || list.get(0).getType() != Type.SRV) {
      throw new NotFoundException("No such record: " + makeHostKey(qkey));
    }

    SRVRecord srec = (SRVRecord) list.get(0);
    return new DNSServiceRecord(srec.rdataToString());
  }

  public String getTXT(String key) throws ConfigException {
    String qkey = fullyQualify(key);
    List<Record> list = m_records.get(makeHostKey(qkey));
    if (list == null || list.get(0).getType() != Type.TXT) {
      throw new NotFoundException("No such record: " + makeHostKey(qkey));
    }

    TXTRecord trec = (TXTRecord) list.get(0);

    String sdata = trec.rdataToString();
    if (sdata.charAt(0) == '"') {
      sdata = sdata.substring(1, sdata.length() - 1);
    }
    return sdata;
  }

  public void setSource(Object source) throws ConfigException {
    try {
      Master m;
      if (source instanceof InputStream)
        m = new Master((InputStream) source);
      else if (source instanceof String)
        m = new Master(ConfigUtils.getInitialPropertyExpanded((String) source));
      else
        throw new ConfigException("not InputStream or String file name: " + source);
      initialize(m);
    }
    catch (IOException e) {
      throw new ConfigException("cannot initialize dns with " + source, e);
    }
    LOGGER.info( "Initializing transient DNS source from " + source);
    m_source = source;
  }

  private void initialize(Master master) throws IOException {
    m_records = new HashMap<String, List<Record>>();
    for (Record rec = null; (rec = master.nextRecord()) != null;) {
      String name = rec.getName().toString();
      List<Record> list = m_records.get(name);
      if (list == null) {
        list = new ArrayList<Record>();
        m_records.put(name, list);
      }
      list.add(rec);
    }
  }

  private String makeHostKey(String host) {
    return host.charAt(host.length() - 1) != '.' ? host + "." + getDomain() + "." : host;
  }

  private List<InetAddress> resolve(String key) throws UnknownHostException {
    List<InetAddress> result = new ArrayList<InetAddress>();
    List<Record> list = m_records.get(key);
    if (list != null)
      for (Record r : list)
    	
        if (r.getType() == Type.A)
        	result.add(InetAddress.getByName(r.rdataToString()));
        else if ((r.getType() == Type.CNAME)) {
        	List<Record> cnamelist = m_records.get(r.rdataToString());
        	for (Record r1 : cnamelist) {
        		result.add(InetAddress.getByName(r1.rdataToString()));
        	}
        }
        
     

    return result;
  }
}
