/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser;

import java.io.StringWriter;
import java.io.Writer;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Element;

//FIXME It was moved out of Sun internal package to get rid of compiler warnings. Please re-implement, see http://xerces.apache.org/xerces2-j/javadocs/other/org/apache/xml/serialize/XMLSerializer.html for more details
public class XHelper {
  public static String getStringFromElement(Element element) {
    StringWriter sw = new StringWriter();
    saveElementToWriter(element, sw);
    String returnString = sw.toString();
    return returnString;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DE_MIGHT_IGNORE")
  public static void saveElementToWriter(Element element, Writer writer) {
    if (element == null) {
      return;
    }

    DOMSource domSource = new DOMSource(element.getOwnerDocument());
    Transformer transformer;
    try {
        transformer = TransformerFactory.newInstance().newTransformer();
    } catch (TransformerConfigurationException e1) {
        return;
    } catch (TransformerFactoryConfigurationError e1) {
        return;
    }
    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
    transformer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    StreamResult sr = new StreamResult(writer);
    try {
        transformer.transform(domSource, sr);
    } catch (TransformerException e1) {
        return;
    }
    
  }

}
