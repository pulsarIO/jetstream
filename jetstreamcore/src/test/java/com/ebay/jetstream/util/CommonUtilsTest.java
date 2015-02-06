/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

public class CommonUtilsTest {

  public static class TestRunnable implements Runnable {
    private Long timeLong1 = null;
    private Long timeLong2 = null;
    private Long timeLong3 = null;

    public Long getTimeLong1() {
      return timeLong1;
    }

    public Long getTimeLong2() {
      return timeLong2;
    }

    public Long getTimeLong3() {
      return timeLong3;
    }

    public void run() {
      try {
        for (int i = 0; i < 1000; i++) {
          timeLong1 = DateUtil.getMillisFromISO8601("2009-12-04 23:29:06");
          timeLong2 = DateUtil.getMillisFromYYYYMMDD("2009-12-04");
          timeLong3 = DateUtil.getMillisFromYYYYMMDD("2009-12");
        }
      }
      catch (Exception e) {
        timeLong1 = timeLong2 = timeLong3 = null;
        System.err.println("EXCEPTION IN Thread Id: " + Thread.currentThread().getId() + ": " + e.getMessage());
      }
    }
  }

  private static final int THREADS_NUM = 32;

  public void testDateUtil() throws Exception {
    String time = "2009-12-04T23:29:06"; // 11878 fix
    Long timeLong = DateUtil.getMillisFromISO8601(time);
    Assert.assertNotNull(timeLong);
    Assert.assertEquals(1259949546000L, timeLong.longValue());
    time = "2009-12-04 23:29:06";
    timeLong = DateUtil.getMillisFromISO8601(time);
    Assert.assertNotNull(timeLong);
    Assert.assertEquals(1259949546000L, timeLong.longValue());
    time = "2009-12-04T23:29:06Z";
    timeLong = DateUtil.getMillisFromISO8601(time);
    Assert.assertNotNull(timeLong);
    Assert.assertEquals(1259949546000L, timeLong.longValue());
    time = "2009-12-04 23:29:06Z";
    timeLong = DateUtil.getMillisFromISO8601(time);
    Assert.assertNotNull(timeLong);
    Assert.assertEquals(1259949546000L, timeLong.longValue());

    String date = "2009-12-04";
    Date dt = DateUtil.getDateYYYYMMDD(date);
    Assert.assertNotNull(dt);
    timeLong = DateUtil.getMillisFromYYYYMMDD(date);
    Assert.assertNotNull(timeLong);
    Assert.assertEquals(1259865000000L, timeLong.longValue());

    date = "2009-12";
    dt = DateUtil.getDateYYYYMMDD(date);
    Assert.assertNotNull(dt);
    timeLong = DateUtil.getMillisFromYYYYMMDD(date);
    Assert.assertNotNull(timeLong);
    Assert.assertEquals(1262284199000L, timeLong.longValue());
  }

  public void testDateUtilThreading() throws Exception {
    ExecutorService threadPool = Executors.newCachedThreadPool();
    TestRunnable runnables[] = new TestRunnable[THREADS_NUM];
    try {
      for (int i = 0; i < THREADS_NUM; i++) {
        runnables[i] = new TestRunnable();
        threadPool.submit(runnables[i]);
      }
      threadPool.shutdown();
      threadPool.awaitTermination(10, TimeUnit.SECONDS);

      for (int i = 0; i < THREADS_NUM; i++) {
        Assert.assertNotNull(runnables[i].getTimeLong1());
        Assert.assertEquals(1259949546000L, runnables[i].getTimeLong1().longValue());
        Assert.assertNotNull(runnables[i].getTimeLong2());
        Assert.assertEquals(1259865000000L, runnables[i].getTimeLong2().longValue());
        Assert.assertNotNull(runnables[i].getTimeLong3());
        Assert.assertEquals(1262284199000L, runnables[i].getTimeLong3().longValue());
      }
    }
    catch (InterruptedException e) {
      fail("InterruptedException: " + e.getMessage());
    }
    assertTrue("Test thread pool failed to shut down properly", threadPool.isShutdown());
  }

  public void testGetObjectFromString() throws Exception {
    Assert.assertEquals("this is a test", CommonUtils.getObjectFromString(String.class, "this is a test"));
    Assert.assertEquals(Integer.valueOf(123), CommonUtils.getObjectFromString(Integer.class, "123"));
    Assert.assertEquals(Long.valueOf(123), CommonUtils.getObjectFromString(Long.class, "123"));
    Assert.assertEquals(Double.valueOf(123.456), CommonUtils.getObjectFromString(Double.class, "123.456"));
    Assert.assertEquals(Integer.valueOf(123), CommonUtils.getObjectFromString(int.class, "123"));
    Assert.assertEquals(Long.valueOf(123), CommonUtils.getObjectFromString(long.class, "123"));
    Assert.assertEquals(Double.valueOf(123.456), CommonUtils.getObjectFromString(double.class, "123.456"));
  }

}
