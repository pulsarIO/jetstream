/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.springkernel.spring;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import com.ebay.jetstream.config.RootConfiguration;

/**
 * 
 * 
 */
public class SpringURLConnection extends URLConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpringURLConnection.class.getCanonicalName());

  private InputStream m_object;

  /**
   * @param url
   */
  public SpringURLConnection(URL url) {
    super(url);
    LOGGER.info( "SPRING URL: " + url);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.net.URLConnection#connect()
   */
  @Override
  public void connect() throws IOException {
    if (m_object == null) {
      Object object = null;
      try {
        String beanPath = getURL().getPath().replace('/', '.');
        if (beanPath.startsWith("."))
          beanPath = beanPath.substring(1);
        object = RootConfiguration.get(beanPath);
      }
      catch (NoSuchBeanDefinitionException e) {
        LOGGER.info( "SPRING URL DOES NOT EXIST: " + url);
        throw new IOException(getURL() + " does not exist: " + e);
      }
      if (!(object instanceof InputStream))
        throw new IOException(getURL() + " is not an InputStream");
      m_object = (InputStream) object;
      LOGGER.info( "SPRING URL FOUND: " + url);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.net.URLConnection#getInputStream()
   */
  @Override
  public InputStream getInputStream() throws IOException {
    connect();
    return m_object;
  }
}
