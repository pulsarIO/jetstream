/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser;

import java.rmi.MarshalException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.springframework.util.ClassUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public abstract class XMLSerializer implements IXmlSerializer {
  private final Map<Class<?>, IXmlSerializer> m_nodeSerializers = new HashMap<Class<?>, IXmlSerializer>();

  protected Element getRootElement(Object object) {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder;
    try {
        builder = factory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
        throw new IllegalStateException (e);
    }
    Document document = builder.newDocument();
    return (Element) document.appendChild(document.createElement("bean"));
  }

  protected IXmlSerializer getSerializer(Object object, Class<?> defaultClass) {
    Class<?> clazz = object == null || ClassUtils.isPrimitiveOrWrapper(object.getClass()) ? null : object.getClass();
    IXmlSerializer result = m_nodeSerializers.get(clazz);
    if (result == null) {
      for (Map.Entry<Class<?>, IXmlSerializer> entry : m_nodeSerializers.entrySet()) {
        Class<?> c = entry.getKey();
        if (c != null && c != Object.class && c.isAssignableFrom(clazz)) {
          result = entry.getValue();
          break;
        }
      }
      // Handle XSerializable implicitly and explicitly (note implicit was already checked)
      if (result == null && XMLSerializationManager.isXSerializable(object.getClass())) {
        result = m_nodeSerializers.get(XSerializable.class);
      }
      // Finally, use the default if nothing better found
      if (result == null) {
        result = m_nodeSerializers.get(defaultClass);
      }
    }
    return result;
  }

  /**
   * Serializes an object to its XML representation, in a DOM.
   * 
   * @param object
   *          the object to serialize.
   * @return the root Element of a DOM containing the serialized object.
   */
  public Element getXMLRepresentation(Object object) {
    Element root = getRootElement(object);
    try {
      getSerializer(object, XSerializable.class).serialize(root, object);
    }
    catch (MarshalException e) {
      throw new RuntimeException(e);
    }
    return root;
  }

  /**
   * Serializes an object to its XML representation, as a String.
   * 
   * @param object
   *          the object to serialize.
   * 
   * @return the serialized XML representation of an object as a String.
   */
  public String getXMLStringRepresentation(Object object) {
    return XHelper.getStringFromElement(getXMLRepresentation(object));
  }

  /**
   * Registers a serializer for a class. There are several "special" classes: null represents the primitive object
   * serializer. Object.class represents the basic object serializer. XSerializable represents the bean serializer.
   * 
   * @param clazz
   *          the class to register for.
   * @param serializer
   *          the instance of IXmlSerializer to use for this class.
   * @return the old serializer.
   */
  protected IXmlSerializer registerSerializer(Class<?> clazz, IXmlSerializer serializer) {
    IXmlSerializer old = m_nodeSerializers.get(clazz);
    m_nodeSerializers.put(clazz, serializer);
    return old;
  }

  public void serialize(Element containingElement, Object object) throws MarshalException {
    getSerializer(object, Object.class).serialize(containingElement, object);
  }
}
