/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser;

class XmlSerTestAddress implements XSerializable {
  public enum Category {
    HOME, BUSINESS
  };

  private String m_street;
  private String m_city;
  private Category m_category;

  public XmlSerTestAddress() {
    m_street = "2145 Ham Ave";
    m_city = "San Jose";
    m_category = Category.BUSINESS;
  }

  public Category getCategory() {
    return m_category;
  }

  public String getCity() {
    return m_city;
  }

  public String getStreet() {
    return m_street;
  }

  public void setCategory(Category category) {
    m_category = category;
  }

  public void setCity(String city) {
    m_city = city;
  }

  public void setStreet(String street) {
    m_street = street;
  }
}
