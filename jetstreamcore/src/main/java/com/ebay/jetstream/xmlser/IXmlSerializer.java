/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser;

import java.rmi.MarshalException;

import org.w3c.dom.Element;

public interface IXmlSerializer {
  public void serialize(Element containingElement, Object object) throws MarshalException;
}
