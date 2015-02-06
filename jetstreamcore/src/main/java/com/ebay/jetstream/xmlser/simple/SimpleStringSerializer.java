/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.simple;

import java.rmi.MarshalException;

import org.w3c.dom.Element;

import com.ebay.jetstream.xmlser.IXmlSerializer;

public class SimpleStringSerializer implements IXmlSerializer {
  public void serialize(Element containingElement, Object object) throws MarshalException {
    containingElement.appendChild(containingElement.getOwnerDocument().createTextNode(object + ""));
  }
}
