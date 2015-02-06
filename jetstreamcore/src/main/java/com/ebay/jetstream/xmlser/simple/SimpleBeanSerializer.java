/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.simple;

import java.rmi.MarshalException;

import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.AbstractBeanSerializer;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class SimpleBeanSerializer extends AbstractBeanSerializer {
  @Override
  public void serialize(Element containerElement, Object object) throws MarshalException {
    containerElement.setAttribute("class", object.getClass().getCanonicalName());
    super.serialize(containerElement, object);
  }

  @Override
  protected void serializeProperty(Element containerElement, String propertyName, Object propertyValue)
      throws MarshalException {
    Element propertyElement = containerElement.getOwnerDocument().createElement(propertyName);
    containerElement.appendChild(propertyElement);
    XMLSerializationManager.getSerializer("xml").serialize(propertyElement, propertyValue);
  }
}
