/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import org.springframework.beans.factory.NamedBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * This is an event class for all the bean changes that happen in Ldap
 * 
 * @author jvembunarayanan
 * 
 */
public class ContextBeanChangedEvent extends ApplicationContextEvent {
  private static final long serialVersionUID = 1L;

  private final String m_beanName;

  public ContextBeanChangedEvent(ApplicationContext context, String beanName) {
    super(context);
    m_beanName = beanName;
  }

  public String getBeanName() {
    return m_beanName;
  }

  public Object getChangedBean() {
    return getApplicationContext().getBean(getBeanName());
  }

  public boolean isChangedBean(NamedBean bean) {
    return bean != null && getBeanName().equals(bean.getBeanName());
  }
}
