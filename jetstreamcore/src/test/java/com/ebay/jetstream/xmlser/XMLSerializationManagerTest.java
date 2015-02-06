/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.util.CommonUtils;
import com.ebay.jetstream.xmlser.spring.SpringBeanDeserializer;

public class XMLSerializationManagerTest {
  /**
   * Prints the current actual results
   *
   * @param argv
   */
  public static void main(String[] argv) {
    XmlSerTestPerson xmlSerTestPerson = new XmlSerTestPerson();
    System.out.println("\ntestSimpleSerialize.xml:\n"); //KEEPME
    System.out.println(XMLSerializationManager.getSerializer("xml").getXMLStringRepresentation(xmlSerTestPerson)); //KEEPME
    System.out.println("\ntestSpringSerialize.xml:\n"); //KEEPME
    System.out.println(XMLSerializationManager.getSerializer("spring").getXMLStringRepresentation(xmlSerTestPerson)); //KEEPME
  }

  public void test01SimpleSerialize() throws Exception {
    XmlSerTestPerson xmlSerTestPerson = new XmlSerTestPerson();
    String expected = CommonUtils.getResourceAsString(getClass(), "testSimpleSerialize.xml", "\n");
    String actual = XMLSerializationManager.getSerializer("xml").getXMLStringRepresentation(xmlSerTestPerson);
    Assert.assertEquals(expected, actual);
  }

  public void test02SpringSerialize() throws Exception {
    XmlSerTestPerson xmlSerTestPerson = new XmlSerTestPerson();
    String expected = CommonUtils.getResourceAsString(getClass(), "testSpringSerialize.xml", "\n");
    String actual = XMLSerializationManager.getSerializer("spring").getXMLStringRepresentation(xmlSerTestPerson);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void test03SpringDeserialize() throws Exception {
    XmlSerTestPerson person1 = new XmlSerTestPerson("Ricky Ho");
    XmlSerTestPerson person2 = new XmlSerTestPerson();
    person2.setFullName("Mark Sikes");
    XMLSerializer xs = XMLSerializationManager.getSerializer("spring");
    String content = "";
    content += xs.getXMLStringRepresentation(person1);
    content += xs.getXMLStringRepresentation(person2);
    Map<String, Object> result = new SpringBeanDeserializer().deserialize(content);
    for (Map.Entry<String, Object> entry : result.entrySet()) {
      System.out.println("Deserialized key = " + entry.getKey()); //KEEPME
      System.out.println(" Value = " + xs.getXMLStringRepresentation(entry.getValue())); //KEEPME
    }
  }
}
