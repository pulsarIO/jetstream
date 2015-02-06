/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

/**
 * 
 * ConfigChangeMessage is a class which represent a message which is tied to a topic called
 * "Jetstream.global\configChange"
 * 
 * @author mvembunarayanan
 */

public class ConfigChangeMessage extends JetstreamMessage implements BeanChangeInformation {
  private static final long serialVersionUID = 1L;
  private String m_application;
  private String m_version;
  
  private String m_scope;
  private String m_bean;
  private String m_beanVersion;
  
  private long m_masterLdapVersion;

  public ConfigChangeMessage() {
	  super();
  }

  /**
   * 
   * @param application
   * @param scope
   * @param version
   * @param bean
   */
  public ConfigChangeMessage(String application, String scope, String version, String bean, long masterLdapVersion) {
    m_application = application;
    m_scope = scope;
    m_version = version;
    m_bean = bean;
    m_masterLdapVersion = masterLdapVersion;
  }
  
  public ConfigChangeMessage(String application, String scope, String version, String bean, String beanVersion) {
    m_application = application;
    m_scope = scope;
    m_version = version;
    
    m_bean = bean;
    m_beanVersion = beanVersion;
  }

  public String getApplication() {
    return m_application;
  }

  public String getBeanName() {
    return m_bean;
  }

  public String getScope() {
    return m_scope;
  }

  public String getVersionString() {
    return m_version;
  }
  
  public long getMasterLdapVersion(){
    return m_masterLdapVersion;
  }
  
  public String getBeanVersion() {
	  return m_beanVersion;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    super.readExternal(in);
    try {
      m_application = (String) in.readObject();
      m_scope = (String) in.readObject();
      m_version = (String) in.readObject();
      m_bean = (String) in.readObject();
      m_beanVersion = (String) in.readObject();
      m_masterLdapVersion = in.readLong();
    }
    catch (ClassNotFoundException e) { // NOPMD
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeObject(m_application);
    out.writeObject(m_scope);
    out.writeObject(m_version);
    out.writeObject(m_bean);
    out.writeObject(m_beanVersion);
    out.writeLong(m_masterLdapVersion);
  }

}
