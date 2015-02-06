/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.spring;

import java.rmi.MarshalException;
import java.util.Collection;
import java.util.Map;

import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.IXmlSerializer;
import com.ebay.jetstream.xmlser.XMLSerializer;
import com.ebay.jetstream.xmlser.XSerializable;

public class SpringXMLSerializer extends XMLSerializer {
  private String m_defaultScope = null;
  private boolean m_defaultLazyInit = true;

  public SpringXMLSerializer() {
    registerSerializer(null, new SpringStringSerializer());
    registerSerializer(Object.class, new SpringStringSerializer());
    registerSerializer(XSerializable.class, new SpringBeanSerializer());
    registerSerializer(Enum.class, new SpringStringSerializer());
    registerSerializer(String.class, new SpringStringSerializer());
    registerSerializer(Collection.class, new SpringCollectionSerializer());
    registerSerializer(Map.class, new SpringMapSerializer());
  }

  public String getDefaultScope() {
    return m_defaultScope;
  }

  public boolean isDefaultLazyInit() {
    return m_defaultLazyInit;
  }

  @Override
  public void serialize(Element containingElement, Object object) throws MarshalException {
    Element element = containingElement;
    IXmlSerializer xs = getSerializer(object, Object.class);
    if (xs.getClass() == SpringBeanSerializer.class && !element.getNodeName().equals("bean")) {
      element = containingElement.getOwnerDocument().createElement("bean");
      containingElement.appendChild(element);
    }
    xs.serialize(element, object);
  }

  public void setDefaultLazyInit(boolean lazyInit) {
    m_defaultLazyInit = lazyInit;
  }

  public void setDefaultScope(String scope) {
    m_defaultScope = scope;
  }
}
