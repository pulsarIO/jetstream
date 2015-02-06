/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.simple;

import java.rmi.MarshalException;
import java.util.Collection;

import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.IXmlSerializer;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class SimpleCollectionSerializer implements IXmlSerializer {
  public void serialize(Element containingElement, Object object) throws MarshalException {
    Collection<?> collection = (Collection<?>) object;
    for (Object item : collection) {
      Element itemElement = containingElement.getOwnerDocument().createElement("item");
      containingElement.appendChild(itemElement);
      if (item != null) {
        itemElement.setAttribute("class", item.getClass().getSimpleName());
      }
      XMLSerializationManager.getSerializer("xml").serialize(itemElement, item);
    }
  }
}
