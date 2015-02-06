/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.application;

public class JetstreamApplicationTest extends JetstreamApplication {

  static {
    setApplicationClass(JetstreamApplicationTest.class);
  }

  public JetstreamApplicationTest() {
    JetstreamApplicationInformation ai = getApplicationInformation();
    ai.setConfigVersion("0.0");
    ai.setBeans(null);
  }

  @Override
  protected void run() {
    try {
      shutdown();
    }
    catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}