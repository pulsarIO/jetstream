/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/

package com.ebay.jetstream.spring.context.support;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.ApplicationEvent;

/**
 * @author msubbaiah
 * 
 */
public class AbstractApplicationEvent extends ApplicationEvent {

  /**
   * 
   */
  private static final long serialVersionUID = -7743367213331666054L;

  private final Map<String, Object> m_metaData = new HashMap<String, Object>();

  public AbstractApplicationEvent(Object source) {
    super(source);
  }

  /**
   * 
   * @param key
   * @param value
   */
  public void addMetaData(String key, Object value) {
    m_metaData.put(key, value);
  }

  /**
   * 
   * @return
   */
  public Map<String, Object> getMetaData() {
    return m_metaData;
  }

}
