/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.spring;

import java.rmi.MarshalException;
import java.util.Map;

import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.IXmlSerializer;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class SpringPropertySerializer implements IXmlSerializer {
  public void serialize(Element containingElement, Object object) throws MarshalException {
    Map<?, ?> map = (Map<?, ?>) object;
    Element mapElement = containingElement.getOwnerDocument().createElement("map");
    containingElement.appendChild(mapElement);

    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String key = entry.getKey().toString();
      Object value = entry.getValue();

      Element entryElement = mapElement.getOwnerDocument().createElement("entry");
      entryElement.setAttribute("key", key);
      mapElement.appendChild(entryElement);
      XMLSerializationManager.getSerializer("spring").serialize(entryElement, value);
    }
  }

}
