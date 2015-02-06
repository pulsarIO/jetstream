/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.spring;

import java.rmi.MarshalException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.IXmlSerializer;

public class SpringStringSerializer implements IXmlSerializer {
  public void serialize(Element containingElement, Object object) throws MarshalException {
    Document d = containingElement.getOwnerDocument();
    String value = object + "";
    String name = containingElement.getNodeName();
    if (object == null || name.equals("list") || name.equals("set") || name.equals("entry")) {
      Element element = d.createElement(object == null ? "null" : "value");
      containingElement.appendChild(element);
      if (object != null) {
        element.appendChild(d.createTextNode(value));
      }
    }
    else {
      containingElement.setAttribute("value", value);
    }
  }

}
