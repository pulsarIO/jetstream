/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management;

import java.io.IOException;

/**
 * 
 */
public class XmlResourceFormatter extends AbstractResourceFormatter {
  public XmlResourceFormatter() {
    this("xml");
  }

  public XmlResourceFormatter(String serializationFormat) {
    super(serializationFormat);
  }

  public String getContentType() {
    return "text/xml";
  }

  @Override
  protected void beginFormat() {
    pushElement("beans", null);
  }

  @Override
  protected void endFormat() {
    popElement();
  }

  @Override
  protected void formatBean(Object bean) throws IOException {
    getWriter().println(getSerializer().getXMLStringRepresentation(bean));
  }

  @Override
  protected void formatReference(String key) throws IOException {
    getWriter().println("<bean ref=\"" + getReference(key) + "\"/>");
  }
}
