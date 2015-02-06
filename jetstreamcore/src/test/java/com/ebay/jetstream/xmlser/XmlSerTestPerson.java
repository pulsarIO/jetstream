/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ebay.jetstream.xmlser.XmlSerTestAddress.Category;

public class XmlSerTestPerson {
  private String m_fullName;
  private int m_age;
  private List<String> m_hobbies = new ArrayList<String>();
  private Properties m_scores = new Properties();
  private XmlSerTestAddress m_residence = new XmlSerTestAddress();
  private Map<String, XmlSerTestAddress> m_homes = new HashMap<String, XmlSerTestAddress>();

  public XmlSerTestPerson() {
    this("John Doe");
  }

  public XmlSerTestPerson(String name) {
    setFullName(name);
    setAge(35);
    getHobbies().add("soccer");
    getHobbies().add("pingpong");
    getHobbies().add(null); // test
    getScores().put("soccer", 100);
    getScores().put("pingpong", 70);
    getResidence().setCategory(Category.HOME);
    XmlSerTestAddress addrTest = new XmlSerTestAddress();
    addrTest.setCategory(null);
    getHomes().put("USA", new XmlSerTestAddress());
    getHomes().put("India", addrTest);
    getHomes().put("nullTest", null);
  }

  public int getAge() {
    return m_age;
  }

  public String getFullName() {
    return m_fullName;
  }

  @Hidden
  public String getHidden() {
    return toString();
  }

  public List<String> getHobbies() {
    return m_hobbies;
  }

  public Map<String, XmlSerTestAddress> getHomes() {
    return m_homes;
  }

  public XmlSerTestAddress getResidence() {
    return m_residence;
  }

  public Properties getScores() {
    return m_scores;
  }

  public void setAge(int age) {
    m_age = age;
  }

  public void setFullName(String fullName) {
    m_fullName = fullName;
  }

  public void setHobbies(List<String> hobbies) {
    m_hobbies = hobbies;
  }

  public void setHomes(Map<String, XmlSerTestAddress> homes) {
    m_homes = homes;
  }

  public void setResidence(XmlSerTestAddress residence) {
    m_residence = residence;
  }

  public void setScores(Properties scores) {
    m_scores = scores;
  }
}
