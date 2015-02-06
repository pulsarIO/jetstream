/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.util.HashMap;

import org.springframework.beans.BeansException;

import com.ebay.jetstream.xmlser.XSerializable;

public class ApplicationInformation extends HashMap<String, String> implements XSerializable {
  private static final long serialVersionUID = 1L;

  public ApplicationInformation() {
  }

  public ApplicationInformation(String applicationName, String configVersion) {
    setApplicationName(applicationName);
    setConfigVersion(configVersion);
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  public String getApplicationName() {
    return get("applicationName");
  }

  public String getConfigVersion() {
    return get("configVersion");
  }

  public void onRefresh(Configuration configuration) throws BeansException {
    // NOP by default
  }

  @Override
  public String put(String key, String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String remove(Object key) {
    throw new UnsupportedOperationException();
  }

  public void setApplicationName(String name) {
    set("applicationName", name);
  }

  public void setConfigVersion(String version) {
    set("configVersion", version);
  }

  protected void set(String key, String value) {
    super.put(key, value);
  }
}
