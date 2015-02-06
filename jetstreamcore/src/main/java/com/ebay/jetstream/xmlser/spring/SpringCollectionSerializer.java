/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.spring;

import java.rmi.MarshalException;
import java.util.Collection;
import java.util.Set;

import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.IXmlSerializer;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class SpringCollectionSerializer implements IXmlSerializer {
  public void serialize(Element containingElement, Object object) throws MarshalException {
    Collection<?> collection = (Collection<?>) object;
    String elementName = collection instanceof Set ? "set" : "list";
    Element listElement = containingElement.getOwnerDocument().createElement(elementName);
    containingElement.appendChild(listElement);
    for (Object item : collection) {
      XMLSerializationManager.getSerializer("spring").serialize(listElement, item);
    }
  }
}
