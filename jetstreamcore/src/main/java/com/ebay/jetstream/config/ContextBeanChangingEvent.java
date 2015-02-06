/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * This is an event class for all the bean changes that happen in Ldap
 * 
 * 
 * 
 */
public class ContextBeanChangingEvent extends ApplicationContextEvent {
  private static final long serialVersionUID = 1L;
  private final BeanChangeInformation m_beanChangeInformation;

  public ContextBeanChangingEvent(ApplicationContext context, BeanChangeInformation beanChangeInformation) {
    super(context);
    m_beanChangeInformation = beanChangeInformation;
  }

  public BeanChangeInformation getBeanChangeInformation() {
    return m_beanChangeInformation;
  }
}
