/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.xmlser;

import java.rmi.MarshalException;
import java.util.Map;

/**
 * 
 */
public interface IXmlDeserializer {
  Map<String, Object> deserialize(String source) throws MarshalException;
}
