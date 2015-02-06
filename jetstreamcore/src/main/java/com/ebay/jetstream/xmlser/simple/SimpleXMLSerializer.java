/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.xmlser.simple;

import java.util.Collection;
import java.util.Map;

import com.ebay.jetstream.xmlser.XMLSerializer;
import com.ebay.jetstream.xmlser.XSerializable;

public class SimpleXMLSerializer extends XMLSerializer {
  public SimpleXMLSerializer() {
    registerSerializer(null, new SimpleStringSerializer());
    registerSerializer(Object.class, new SimpleStringSerializer());
    registerSerializer(XSerializable.class, new SimpleBeanSerializer());
    registerSerializer(String.class, new SimpleStringSerializer());
    registerSerializer(Enum.class, new SimpleStringSerializer());
    registerSerializer(Collection.class, new SimpleCollectionSerializer());
    registerSerializer(Map.class, new SimpleMapSerializer());
  }
}
