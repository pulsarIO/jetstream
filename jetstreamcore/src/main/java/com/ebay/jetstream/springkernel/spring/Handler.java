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
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * 
 * 
 */
public class Handler extends URLStreamHandler {

  /*
   * (non-Javadoc)
   * 
   * @see java.net.URLStreamHandler#openConnection(java.net.URL)
   */
  @Override
  protected URLConnection openConnection(URL u) throws IOException {
    return new SpringURLConnection(u);
  }

}
