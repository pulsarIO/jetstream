/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class RESTHelper {
  public static String httpGet(String urlString) throws MalformedURLException, IOException {
    URL url = new URL(urlString);
    InputStream in = url.openStream();
    try {
      return CommonUtils.getStreamAsString(in, "\n");
    }
    finally {
      in.close();
    }
  }

  public static String httpPost(String urlString, String data) throws IOException {
    URL url = new URL(urlString);
    URLConnection uc = url.openConnection();
    uc.setDoOutput(true);
    OutputStreamWriter writer = new OutputStreamWriter(uc.getOutputStream());
    InputStream in = null;
    try {
      if (data != null) {
        writer.write(data);
      }
      writer.flush();
      // Get the response
      in = uc.getInputStream();
      return CommonUtils.getStreamAsString(in, "\n");
    }
    finally {
      writer.close();
      if (in != null) {
        in.close();
      }
    }
  }
}
