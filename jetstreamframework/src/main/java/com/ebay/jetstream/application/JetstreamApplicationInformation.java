/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.application;

import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.config.SingletonBeanRegistry;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.Configuration;
import com.ebay.jetstream.config.NICUsage;
import com.ebay.jetstream.config.SelfLocator;
import com.ebay.jetstream.config.dns.DNSMap;
import com.ebay.jetstream.config.dns.impl.DNSFileMap;
import com.ebay.jetstream.config.dns.impl.DNSJNDIMap;
import com.ebay.jetstream.util.CommonUtils;

public class JetstreamApplicationInformation extends ApplicationInformation {
  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final int MAX_WORKQUEUE_SIZE=1000;
	 private static final int MAX_WORKER_THREADS=1;
	 
  private final transient JetstreamApplication m_jetstreamApplication; 
  private String[] m_configRoots;
  private String[] m_beans;
  private transient DNSMap m_dnsMap; 
  private transient NICUsage m_nicUsage; 
  private transient SelfLocator m_selfLocator; 
  private int m_workQueueSz = MAX_WORKQUEUE_SIZE;
  private int m_workerThreads = MAX_WORKER_THREADS;
  
  /**
   * @param jetstreamApplication
   */
  protected JetstreamApplicationInformation(JetstreamApplication jetstreamApplication) {
    m_jetstreamApplication = jetstreamApplication;
    set("usedns", "true");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ebay.jetstream.config.ApplicationInformation#getApplicationName()
   * 
   * If no application name is explicitly set, the name is formed from the outer parent class.
   */
  @Override
  public String getApplicationName() {
    String result = super.getApplicationName();
    if (CommonUtils.isEmptyTrimmed(result)) {
      setApplicationName(result = m_jetstreamApplication.getClass().getSimpleName());
    }
    return result;
  }

  /**
   * @return the beans to load at application start.
   */
  public String[] getBeans() {
    return m_beans;
  }

  /**
   * @return the configRoots
   */
  public String[] getConfigRoots() {
    return m_configRoots;
  }

  /**
   * @return the managementPort
   */
  public int getManagementPort() {
    String mp = get("managementPort");
    return mp != null ? Integer.valueOf(mp).intValue() : 9999;
  }

  /**
   * @return the zone, or null
   */
  public String getZone() {
    return get("zone");
  }

  public boolean useDNS() {
    return get("usedns").equalsIgnoreCase("true");
  }

  @Override
  public void onRefresh(Configuration configuration) throws BeansException {
    super.onRefresh(configuration);
    if (!useDNS()) {
      return;
    }
    SingletonBeanRegistry br = configuration.getBeanFactory();
    try {
      br.registerSingleton("DNSMap", getDnsMap());
      br.registerSingleton("NICUsage", getNicUsage());
      br.registerSingleton("SelfLocator", getSelfLocator());
    }
    catch (ConfigException e) {
      throw new FatalBeanException("ApplicationInformation initialization failed", e);
    }
  }

  public void selfLocate() throws ConfigException {
    if (!useDNS()) {
      return;
    }
    SelfLocator sl = getSelfLocator();
    if (sl.getLdapHost() != null && sl.getLdapPort() != -1) {
      set("ldapHostPort", sl.getLdapHost() + ":" + sl.getLdapPort());
    }
    set("scope", "/" + sl.getCanonicalHostName());
    set("guid", sl.getGuid());
  }

  /**
   * @param beans
   *          the beans to load at application start
   */
  public void setBeans(String[] beans) {
    m_beans = beans;
  }

  /**
   * @param configRoots
   *          the configRoots to set
   */
  public void setConfigRoots(String[] configRoots) {
    m_configRoots = configRoots;
  }

  /**
   * @param managementPort
   *          the managementPort to set
   */
  public void setManagementPort(int managementPort) {
    set("managementPort", String.valueOf(managementPort));
  }

  public void setZone(String zone) {
    set("zone", zone);
  }

  public void useDNS(boolean use) {
    set("usedns", use ? "true" : "false");
  }

  protected DNSMap getDnsMap() throws ConfigException {
    if (m_dnsMap == null) {
      String zone = getZone();
      if (CommonUtils.isEmptyTrimmed(zone))
        m_dnsMap = new DNSJNDIMap(); 
      else {
        m_dnsMap = new DNSFileMap(); 
        ((DNSFileMap) m_dnsMap).setSource(zone);
      }
    }
    return m_dnsMap;
  }

  protected NICUsage getNicUsage() throws ConfigException {
    if (m_nicUsage == null) {
      m_nicUsage = new NICUsage();
      m_nicUsage.setDnsMap(getDnsMap());
    }
    return m_nicUsage;
  }

  protected SelfLocator getSelfLocator() throws ConfigException {
    if (m_selfLocator == null) {
      m_selfLocator = new SelfLocator();
      m_selfLocator.setNicUsage(getNicUsage());
    }
    return m_selfLocator;
  }

	public void setWorkQueueSz(int workQueueSz) {
		m_workQueueSz = workQueueSz;		
	}
	
	public int getWorkQeueSz() {
		return m_workQueueSz;
	}
	
	public void setWorkerThreads(int workerThreads) {
		m_workerThreads = workerThreads;
	}
	
	public int getWorkerThreads() {
		return m_workerThreads;
	}
}