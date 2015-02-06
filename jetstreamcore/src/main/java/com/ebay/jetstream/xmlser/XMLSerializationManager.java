/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ebay.jetstream.xmlser.simple.SimpleXMLSerializer;
import com.ebay.jetstream.xmlser.spring.SpringBeanDeserializer;
import com.ebay.jetstream.xmlser.spring.SpringXMLSerializer;

public class XMLSerializationManager {
  private static final Map<String, XMLSerializer> s_serializers = new HashMap<String, XMLSerializer>();
  private static final Map<String, IXmlDeserializer> s_deserializers = new HashMap<String, IXmlDeserializer>();
  private static final List<Class<?>> s_xserializable = new ArrayList<Class<?>>();

  static {
    registerSerializer("xml", new SimpleXMLSerializer());
    registerSerializer("spring", new SpringXMLSerializer());
    registerDeserializer("spring", new SpringBeanDeserializer());
  }

  public static IXmlDeserializer getDeserializer(String format) {
    return s_deserializers.get(format);
  }

  public static XMLSerializer getSerializer(String format) {
    return s_serializers.get(format);
  }

  public static boolean isHidden(Method getter) {
    return getter == null || getter.isAnnotationPresent(Hidden.class) || getter.getParameterTypes().length != 0
        || getter.getName().equals("getClass");
  }

  /**
   * Returns true if classes of this object should be considered to be XML serializable.
   * 
   * @param clazz
   *            the class to check
   * @return true if the class is XSerializable, or is explicitly listed as serializable.
   */
  public static boolean isXSerializable(Class<?> clazz) {
    if (XSerializable.class.isAssignableFrom(clazz))
      return true;
    for (Class<?> explicitx : s_xserializable)
      if (explicitx.isAssignableFrom(clazz))
        return true;
    return false;
  }

  public static IXmlDeserializer registerDeserializer(String format, IXmlDeserializer deserializer) {
    IXmlDeserializer old = s_deserializers.get(format);
    s_deserializers.put(format, deserializer);
    return old;
  }

  public static XMLSerializer registerSerializer(String format, XMLSerializer serializer) {
    XMLSerializer old = s_serializers.get(format);
    s_serializers.put(format, serializer);
    return old;
  }

  /**
   * Adds a class to the explicit list of serializable classes. If the class implements XSerializable, it is already
   * implicitly serializable. This mechanism exists for classes that should be serialized but the source is unavailable
   * and extending isn't an appropriate option.
   * 
   * @param clazz
   *            the class to add.
   * @return true iff the class is added.
   */
  public static boolean registerXSerializable(Class<?> clazz) {
    boolean add = !s_xserializable.contains(clazz);
    if (add)
      s_xserializable.add(clazz);
    return add;
  }
}
