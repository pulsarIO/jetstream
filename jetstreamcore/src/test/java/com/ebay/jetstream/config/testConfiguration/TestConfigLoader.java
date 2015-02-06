/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.testConfiguration;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.Configuration;

public class TestConfigLoader {

  private static String s_classPathLoc = "classpath:com/ebay/jetstream/config/testConfiguration/ConfigSpring.xml";
  private static String s_filePathLoc = "src/test/FilePath/ConfigSpring.xml";
  private static String s_fPathVal = "fromFilePath";
  private static String s_cPathVal = "fromClassPath";
  private static String s_bean = "entryBean";

  private static ApplicationInformation getAppInfo(String app, String host, String version) {
    ApplicationInformation appInfo = new ApplicationInformation();
    appInfo.setApplicationName(app);
    appInfo.setConfigVersion(version);
    return appInfo;
  }

  private static Configuration getConfiguration(String[] loc, ApplicationInformation app) {
    return app == null ? new Configuration(loc) : new Configuration(app, loc);
  }

  private static String[] getLoc(String path) {
    return new String[] { path };
  }

  @Test
  public void checkClassPath() {
    System.out.println("Entering ClassPathValidation");
    TestBean obj = (TestBean) getConfiguration(getLoc(s_classPathLoc), null).getBean(s_bean);
    assertEquals(s_cPathVal, obj.getSource());
  }

  @Test
  public void checkFilePath() {
    System.out.println("Entering FilePathValidation");
    TestBean obj = (TestBean) getConfiguration(getLoc(s_filePathLoc),
        getAppInfo("TIS", "dev05.sol001.dev05", "version_4.0")).getBean(s_bean);
    assertEquals(s_fPathVal, obj.getSource());
  }

}
