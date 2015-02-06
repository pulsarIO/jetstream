/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.springtest.nameidcheck;

import org.springframework.beans.factory.BeanNameAware;

public class InnerBean implements BeanNameAware{
  private String name;
  private String value;

 public String getBeanName() {
    return name;
  }

  public void setBeanName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

}
