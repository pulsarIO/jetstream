/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser;

import java.lang.reflect.Method;
import java.rmi.MarshalException;

import org.w3c.dom.Element;

public abstract class AbstractBeanSerializer implements IXmlSerializer {
  public void serialize(Element containerElement, Object object) throws MarshalException {
    for (Method method : object.getClass().getMethods()) {
      String name = getPropertyName(method);
      if (name != null) {
        method.setAccessible(true); // For private/package class with public methods
        try {
          serializeProperty(containerElement, name, method.invoke(object));
        }
        catch (Exception e) {
          containerElement.appendChild(containerElement.getOwnerDocument().createComment(
              "get failed for " + name + ": " + e));
        }
      }
    }
  }

  protected String getPropertyName(Method getter) {
    String name = getter.getName();
    if (!XMLSerializationManager.isHidden(getter)) {
      if (name.startsWith("get") && name.length() > 3)
        return name.substring(3);

      if (name.startsWith("is") && name.length() > 2 && getter.getReturnType() == Boolean.class)
        return name.substring(2);
    }
    return null;
  }

  protected abstract void serializeProperty(Element containerElement, String propertyName, Object propertyValue)
      throws MarshalException;
}
