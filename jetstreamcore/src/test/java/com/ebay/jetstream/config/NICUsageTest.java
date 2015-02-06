/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class NICUsageTest {

  static NICUsage nic;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    new RootConfiguration(new ApplicationInformation("Test", "0.0.0.0"),
        new String[] { "classpath:com/ebay/jetstream/config/TestConfiguration.xml" });
    nic = (NICUsage) RootConfiguration.get("NICUsage");
    // nic.registerDnsAssignedType("webdmin");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  // @Test
  public void testGetNetworkInterfaceInfoList() {
    List<NetworkInterfaceInfo> list = nic.getNICUsageList();
    assertTrue(list.size() > 0);
  }

}
