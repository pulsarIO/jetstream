/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.management.Management.BeanFolder;
import com.ebay.jetstream.xmlser.XmlSerTestPerson;

/**
 * 
 *
 */
public class ManagementTest {

  @ManagedResource(objectName = "persons/fixed")
  static class AnnotatedFixedPerson extends XmlSerTestPerson {
    AnnotatedFixedPerson() {
    }

    AnnotatedFixedPerson(String name) {
      super(name);
    }
  }

  @ManagedResource("./personA")
  static class AnnotatedRelativePerson extends XmlSerTestPerson {
    AnnotatedRelativePerson(String name) {
      super(name);
    }
  }

  @ManagedResource("persons/fixed/person")
  static class AnnotatedSingletonPerson extends XmlSerTestPerson {
    AnnotatedSingletonPerson(String name) {
      super(name);
    }
  }

  private static Object s_testBean;

  @BeforeClass
  public static void setup() {
    // Add non-annotated beans
    Management.addBean("first/second/personA", new XmlSerTestPerson("Ricky Ho"));
    Management.addBean("first/second/personB", new XmlSerTestPerson("Mark Sikes"));
    Management.addBean("first/personC", new XmlSerTestPerson("Middle Manager"));
    // Add annotated relative beans
    Management.addBean(new AnnotatedRelativePerson("Top Person"));
    Management.addBean("persons", new AnnotatedRelativePerson("First Person"));
    // Add annotated fixed beans
    Management.addBean(new AnnotatedSingletonPerson("Singleton Person"));
    Management.addBean("personX", s_testBean = new AnnotatedFixedPerson("Fixed PersonX"));
  }

  /**
   * Test method for {@link com.ebay.jetstream.management.Management#addBean(java.lang.String, java.lang.Object)}.
   */
  @Ignore
  public void testAddBean() {
    // check non-annotated beans
    checkExists("first/second/personA", "Ricky Ho");
    checkExists("first/second/personB", "Mark Sikes");
    checkExists("first/personC", "Middle Manager");
    // check annotated relative beans
    checkExists("personA", "Top Person");
    checkExists("persons/personA", "First Person");
    // check annotated fixed beans
    checkExists("persons/fixed/person", "Singleton Person");
    checkExists("persons/fixed/personX", "Fixed PersonX");
    Object bad = new XmlSerTestPerson("bad person");
    // empty path element detect
    try {
      Management.addBean("/bad/foo", bad);
      Assert.fail("missing path detection");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("empty path level"));
    }
    // collision detect
    try {
      Management.addBean("first/second/personB", bad);
      Assert.fail("collision detection");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("bean already exists"));
    }
    // bean in the middle detect
    try {
      Management.addBean("first/second/personA/bad", bad);
      Assert.fail("bean in the middle detection");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("expecting BeanFolder"));
    }
  }

  /**
   * Test method for {@link com.ebay.jetstream.management.Management#getBeanOrFolder(java.lang.String)}.
   */
  @Ignore
  public void testGetBeanOrFolder() {
    Object o;
    o = Management.getBeanOrFolder("first");
    Assert.assertTrue(o instanceof BeanFolder && ((BeanFolder) o).size() == 2);
    o = Management.getBeanOrFolder("first/second");
    Assert.assertTrue(o instanceof BeanFolder && ((BeanFolder) o).size() == 2);
    o = Management.getBeanOrFolder("first/second/personB");
    Assert.assertTrue(o instanceof XmlSerTestPerson && ((XmlSerTestPerson) o).getFullName().equals("Mark Sikes"));
    // missing object detect
    try {
      o = Management.getBeanOrFolder("first/second/third");
      Assert.fail("missing object detection");
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("no beans exist at "));
    }
  }

  /**
   * Test method for {@link com.ebay.jetstream.management.Management#removeBeanOrFolder(java.lang.String)}.
   */
  @Ignore
  public void testRemoveBeanOrFolder() {
    Assert.assertFalse(Management.removeBeanOrFolder("first/second/personC"));
    Assert.assertTrue(Management.removeBeanOrFolder("first/second/personB"));
    Assert.assertFalse(Management.removeBeanOrFolder("first/second/personB"));
    Management.addBean("first/second/personB/personB2", new XmlSerTestPerson("New B"));
    Assert.assertTrue(Management.removeBeanOrFolder("personX", s_testBean));
    Assert.assertTrue(Management.removeBeanOrFolder("first/second"));
    Assert.assertFalse(Management.removeBeanOrFolder("first/second/personA"));
    Assert.assertFalse(Management.removeBeanOrFolder("first/second"));
    Management.addBean("first/person Z", new XmlSerTestPerson("New Z"));
    Assert.assertTrue(Management.removeBeanOrFolder("first"));
  }

  private void checkExists(String path, String name) {
    // This may fail with a ClassCastException if a bug exists
    XmlSerTestPerson bean = (XmlSerTestPerson) Management.getBeanOrFolder(path);
    Assert.assertEquals(name, bean.getFullName());
  }
}
