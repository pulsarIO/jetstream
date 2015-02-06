/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.dns.DNSMap;

public class SelfLocator {
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.config.SelfLocator");
  private final List<String> m_log = new ArrayList<String>();
  private NICUsage m_nicUsage;
  private String m_domain;
  private String m_canonicalHostName;
  private String m_guid;
  
  private String m_ldapHost;
  private int m_ldapPort = -1;

  /**
   * Gets the real canonical host name; InetAddress.getCanonicalHostname() does not correctly qualify the host as of
   * Java 1.5. Also, since several NICs can exist, several source addresses and thus several host names may exist. The
   * first source that has a DNS TXT record is deemed the official name. That DNS TXT record contains a GUID and the
   * official (potentially redirected) domain to use.
   * 
   * @return the real canonical host name as determined by dns and TXT lookup
   */
  public String getCanonicalHostName() {
    return m_canonicalHostName;
  }

  /**
   * @return the DNSMap to use for all dns queries.
   */
  public DNSMap getDnsMap() {
    return m_nicUsage.getDnsMap();
  }

  /**
   * Gets the guid for this host. The guid is assigned in a DNS TXT record whose key is the calculated canonical host
   * name.
   * 
   * @return the guid for this host
   */
  public String getGuid() {
    return m_guid;
  }

  public String getLdapHost() {
    return m_ldapHost;
  }

  public int getLdapPort() {
    return m_ldapPort;
  }

 
  /**
   * Gets the NICUsage instance. The NICUsage also refers to the official DNSMap to use.
   * 
   * @return the NICUsage to use for NIC finding and netmask filtering.
   */
  public NICUsage getNicUsage() {
    return m_nicUsage;
  }

  /**
   * Gets the Jetstream Point Of Deployment (RtbdPod) for this host. The deployment is specified in a DNS TXT record whose
   * key is "deployment", qualified by the host name and calculated dns domain.
   * 
   * @return the Jetstream Point Of Deployment for this host.
   */
 

  public void setLdapHost(String host) {
    m_ldapHost = host;
  }

  public void setLdapPort(int port) {
    m_ldapPort = port;
  }

  /**
   * Sets the NICUsage, and indirectly the DNSMap to use, and performs location calculations.
   * 
   * @param nicUsage
   *            the NICUsage to use for calculations.
   * 
   * @throws ConfigException
   */
  public void setNicUsage(NICUsage nicUsage) throws ConfigException {
    m_nicUsage = nicUsage;
    selfLocate();
  }

  @Override
  public String toString() {
    return getCanonicalHostName() + "[GUID=" + getGuid() + ", MESH="
        + ", mesh_domain=" + getDomain() + "]";
  }

  /**
   * Gets the calculated domain. The current domain is retrieved by getDnsMap().getDomain().
   * 
   * @see DNSMap.getDomain()
   * 
   * @return the calculated domain.
   */
  protected String getDomain() {
    return m_domain;
  }

  /**
   * Does full self-location and sets derivative properties accordingly. This is called when the NicUsage property is
   * set.
   * 
   * @throws ConfigException
   *             if self-location cannot be completed due to error or missing configuration data.
   */
  protected void selfLocate() throws ConfigException {
    resolveFqdn();
    
    clearLog();
  }

  private void clearLog() {
    m_log.clear();
  }

  private void error(String message) throws ConfigException {
    for (String m : m_log)
      LOGGER.error( m);
    LOGGER.error( message);
    clearLog();
    throw new ConfigException(message);
  }
  

  private void resolveFqdn() throws ConfigException {
    List<InetAddress> listOfP2PInetAddress = m_nicUsage.getAllInetAddresses(null);

    if (listOfP2PInetAddress.isEmpty()) {
      LOGGER.error( "NICUsage did not initialize correctly" + m_nicUsage);
    }
    for (InetAddress addr : listOfP2PInetAddress) {
    	
    	try {
    	
          	m_canonicalHostName = addr.getCanonicalHostName();
        
            if (m_canonicalHostName.endsWith(".")) {
              m_canonicalHostName = m_canonicalHostName.substring(0, m_canonicalHostName.length() - 1);
            }
            
            LOGGER.info("Canonical host name for addr " + addr.toString() + " is " + m_canonicalHostName);
            
            if (!addr.isLoopbackAddress() && (addr.getAddress().length == 4)) {
            	String str[] = m_canonicalHostName.split("\\.", 2);
            	String newDomain = str[1];
            	if ((str != null) && (str.length == 2)) {
            		getDnsMap().setDomain(newDomain);
            	    m_domain = newDomain;
            	    LOGGER.info("Domain set to : " + m_domain);
            	    break;
            	}
            	
            }
            // getDnsMap().getTXT(m_canonicalHostName + "."); // GUID
            // log(Level.INFO, "GUID TXT found. Canonical host name resolved to " + m_canonicalHostName);
            // break;
          }
          catch (Throwable e) {
            m_canonicalHostName = null;
            LOGGER.debug( "Canonical host name resolve of " + addr + " failed: " + e);
          }
           
    }
    if (m_canonicalHostName == null) {
      error("Could not find GUID TXT for any canonical hostname: cannot self-locate");
    }
  }

  private void resolveGuidAndDomain() throws ConfigException { // NOPMD Preserved for later use.
    String fqdn = m_canonicalHostName + ".";
    String guidTXT = getDnsMap().getTXT(fqdn);
    if (guidTXT == null) {
      error("Cannot resolve GUID for fqdn: " + fqdn);
    }

    String[] str = guidTXT.split("=");
    if (str.length == 2 && str[0].equals("GUID")) {
      m_guid = str[1];
    }
    else {
      error("Invalid GUID TXT record: " + guidTXT);
    }

    LOGGER.info("Resolved Guid is " + m_guid);

    // Reset my domain based on what the GUID tells me
    str = m_guid.split("\\.", 2);

    if (str != null && str.length == 2) {
      String newDomain = str[1];
      getDnsMap().setDomain(newDomain);
      m_domain = newDomain;
      LOGGER.info("Domain set to : " + m_domain);
    }
    else {
      error("Invalid GUID: " + m_guid);
    }
  }
  
  public static String findDomainForHost(NICUsage nicUsage) {
	  List<InetAddress> listOfP2PInetAddress = nicUsage.getAllInetAddresses(null);

	  if (listOfP2PInetAddress.isEmpty()) {
		  LOGGER.error( "NICUsage did not initialize correctly" + nicUsage);
	  }

	  String domainName = null;
	  String canonicalHostName = null;
	  for (InetAddress addr : listOfP2PInetAddress) {

		  try {

			  if (!addr.isLoopbackAddress() && (addr.getAddress().length == 4)) {
				  canonicalHostName = addr.getCanonicalHostName();
				  
				  String str[] = canonicalHostName.split("\\.", 2);
				  String newDomain = str[1];
				  if ((str != null) && (str.length == 2)) {
					  domainName = newDomain;	            	   
					  break;
				  }

			  }
			 
		  }
		  catch (Throwable e) { // NOPMD Intentional

		  }

	  }
	  return domainName;

  }
  
  
}
