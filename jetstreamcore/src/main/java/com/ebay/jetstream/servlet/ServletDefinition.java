/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.servlet;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServlet;

import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={"DMI_RANDOM_USED_ONLY_ONCE", "WMI_WRONG_MAP_ITERATOR"})
public class ServletDefinition implements XSerializable {
  private Class<? extends HttpServlet> m_servletClass;
  private String m_urlPath;
  private Map<String, String> m_initParams;

  /**
   * @return the initParams
   */
  public Map<String, String> getInitParams() {
    return m_initParams;
  }

  /**
   * @return the servletClass
   */
  public Class<? extends HttpServlet> getServletClass() {
    return m_servletClass;
  }

  /**
   * @return the urlPath
   */
  public String getUrlPath() {
    return m_urlPath;
  }

  /**
   * @param initParams
   *            the initParams to set
   */
  public void setInitParams(Map<String, String> initParams) {
    if (m_initParams == null) {
      m_initParams = new HashMap<String, String>();
    }
    else {
      m_initParams.clear();
    }
    for (String initParamKey : initParams.keySet()) {
      m_initParams.put(initParamKey, ConfigUtils.getInitialPropertyExpanded(initParams.get(initParamKey)));
    }
  }

  /**
   * @param servletClass
   *            the servletClass to set
   */
  public void setServletClass(Class<? extends HttpServlet> servletClass) {
    m_servletClass = servletClass;
  }

  /**
   * @param urlPath
   *            the urlPath to set
   */
  public void setUrlPath(String urlPath) {
    m_urlPath = urlPath;
  }
}
