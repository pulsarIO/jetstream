/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.config;

import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.management.Management;

/**
 * @author snikolaev
 * 
 */
public class AbstractNamedManagementBean extends AbstractNamedBean implements InitializingBean {

  /*
   * (non-Javadoc)
   * 
   * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
   */
  public void afterPropertiesSet() throws Exception {
    Management.addBean(getBeanName(), this);
  }

}
