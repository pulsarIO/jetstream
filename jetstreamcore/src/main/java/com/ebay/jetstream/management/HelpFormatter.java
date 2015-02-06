/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.util.CommonUtils;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class HelpFormatter extends HtmlResourceFormatter {
  public HelpFormatter() {
    setFormat("help");
  }

  @Override
  protected void formatOperation(Method method) throws IOException {
    PrintWriter pw = getWriter();
    pw.print(method.getName());
    String help = method.getAnnotation(ManagedOperation.class).description();
    if (!CommonUtils.isEmptyTrimmed(help)) {
      pw.print(":  " + help);
    }
    pw.println();
  }

  @Override
  protected void formatProperty(Object bean, PropertyDescriptor pd) throws IOException {
    PrintWriter pw = getWriter();
    Method getter = pd.getReadMethod();
    ManagedAttribute attr = getter.getAnnotation(ManagedAttribute.class);
    Class<?> pclass = pd.getPropertyType();
    String help = attr != null ? attr.description() : "";
    if (XMLSerializationManager.isXSerializable(pclass)) {
      pw.print("XSerializable ");
    }
    pw.print(pclass.getName() + " " + pd.getDisplayName());
    Method setter = pd.getWriteMethod();
    attr = setter == null ? null : setter.getAnnotation(ManagedAttribute.class);
    if (attr != null) {
      help += attr.description();
      pw.print(" <B>[editable]</B>  ");
    }
    pw.println(help);
    pw.println("<P/>");
  }

}