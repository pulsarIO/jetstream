/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.config;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.NamedBean;

import com.ebay.jetstream.xmlser.Hidden;

/**
 * Provides an easy way to create a name-aware Spring bean. Name-aware Spring beans are useful for individual bean
 * change support that Jetstream extends Spring with, for ldap etc.
 * 
 * 
 */
public abstract class AbstractNamedBean implements NamedBean, BeanNameAware {
  private String m_name;

  /*
   * (non-Javadoc)
   * 
   * @see org.springframework.beans.factory.NamedBean#getBeanName()
   */
  @Hidden
  public String getBeanName() {
    return m_name;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
   */
  public void setBeanName(String name) {
    m_name = name;
  }

  @Override
  public String toString() {
    return getBeanName();
  }
}
