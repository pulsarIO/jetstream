/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.spring;

import java.rmi.MarshalException;
import java.util.Map;
import java.util.Properties;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.IXmlSerializer;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class SpringMapSerializer implements IXmlSerializer {
  public void serialize(Element containingElement, Object object) throws MarshalException {
    boolean isProps = object instanceof Properties;
    String entryElementName = isProps ? "prop" : "entry";
    Document doc = containingElement.getOwnerDocument();
    Element mapElement = doc.createElement(isProps ? "props" : "map");
    containingElement.appendChild(mapElement);

    for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
      String key = entry.getKey().toString();
      Object value = entry.getValue();

      Element entryElement = doc.createElement(entryElementName);
      entryElement.setAttribute("key", key);
      mapElement.appendChild(entryElement);

      if (isProps) {
        entryElement.appendChild(doc.createTextNode(value.toString()));
      }
      else {
        XMLSerializationManager.getSerializer("spring").serialize(entryElement, value);
      }
    }
  }
}
