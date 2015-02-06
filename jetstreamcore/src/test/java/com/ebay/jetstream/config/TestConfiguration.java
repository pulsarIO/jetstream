/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.io.Serializable;
import java.util.EventListener;
import java.util.List;

import org.junit.Test;

public class TestConfiguration extends ApplicationInformation implements EventListener, Serializable {
  private static final long serialVersionUID = 1L;

  @Test
  public void testStatics() throws Exception {
    List<String> contexts = Configuration.getContexts(RootConfiguration.getConfigurationRoot());
    System.out.println(contexts);
    contexts = Configuration.getClasspathContexts(TestConfiguration.class);
    System.out.println(contexts);
    RootConfiguration.applicationClass(this, getClass());
    for (String string : RootConfiguration.getDefaultContexts(this))
      System.out.println(string);
  }
}
