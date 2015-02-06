/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.spring;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.rmi.MarshalException;

import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.AbstractBeanSerializer;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class SpringBeanSerializer extends AbstractBeanSerializer {
  private int m_count = 0;

  @Override
  protected String getPropertyName(Method method) {
    String name = super.getPropertyName(method);
    if (name != null && Character.isUpperCase(name.charAt(0)))
      name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
    return name;
  }

  @Override
  public void serialize(Element beanElement, Object object) throws MarshalException {
    if (beanElement.getParentNode().equals(beanElement.getOwnerDocument())) {
      beanElement.setAttribute("id", "e-" + m_count++);
      SpringXMLSerializer serializer = (SpringXMLSerializer) XMLSerializationManager.getSerializer("spring");
      String scope = serializer.getDefaultScope();
      if (scope != null) {
        beanElement.setAttribute("scope", scope);
      }
      beanElement.setAttribute("lazy-init", String.valueOf(serializer.isDefaultLazyInit()));
    }

    if (Proxy.isProxyClass(object.getClass())) {
      String className;
      String[] tokens = object.toString().split("@");
      className = tokens[0];
      beanElement.setAttribute("class", className);
    }
    else
      beanElement.setAttribute("class", object.getClass().getCanonicalName());
    super.serialize(beanElement, object);
  }

  @Override
  protected void serializeProperty(Element beanElement, String propertyName, Object propertyValue)
      throws MarshalException {
    Element propertyElement = beanElement.getOwnerDocument().createElement("property");
    propertyElement.setAttribute("name", propertyName);
    beanElement.appendChild(propertyElement);
    XMLSerializationManager.getSerializer("spring").serialize(propertyElement, propertyValue);
  }
}
