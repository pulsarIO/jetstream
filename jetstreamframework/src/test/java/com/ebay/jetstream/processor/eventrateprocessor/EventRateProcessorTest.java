/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.processor.eventrateprocessor;

//import org.junit.Test;

//import com.ebay.jetstream.event.processor.eventrateprocessor.EventRateProcessor;
//import com.ebay.jetstream.event.processor.eventrateprocessor.EventRateProcessorConfig;

public class EventRateProcessorTest {
/*
  public static void main(String[] args) throws Exception {

    EventRateProcessorTest wlft = new EventRateProcessorTest();

    wlft.runTest(100000, 10);

    System.out.println(".......Test Ended .......");
  }

  private final EventRateProcessor m_rateProcessor;

  public EventRateProcessorTest() {
    EventRateProcessorConfig cfg = new EventRateProcessorConfig();

    // FIXME: removed due to compilation errors
    // cfg.setMaxCacheElements(100000);
    // cfg.setTimeToIdleSecs(10);
    // cfg.setTimeToLiveSecs(10);
    cfg.setEventRateThreshold(10);

    m_rateProcessor = new EventRateProcessor();
    m_rateProcessor.setEventRateProcessorConfig(cfg);

  }

  @Test
  public void executeTest() {

    for (long i = 0; i < 100; i++) {
      if (m_rateProcessor.isRateLimited(i))
        System.out.println("guid = " + i + " - is whitelisted");
      for (int k = 0; k < 20; k++) {
        for (long j = 101; j < 110; j++) {
          if (m_rateProcessor.isRateLimited(i))
            System.out.println("guid = " + i + " - is whitelisted");
        }
      }
    }
  }

  public void runTest(int outerLoop, int innerLoop) {

    System.out.println("...........test started.........");

    for (long i = 0; i < outerLoop; i++) {
      if (m_rateProcessor.isRateLimited(i))
        System.out.println("guid = " + i + " - is whitelisted");

      for (int k = 0; k < innerLoop; k++) {
        for (long j = outerLoop + 1; j < outerLoop + 10; j++) {
          if (m_rateProcessor.isRateLimited(j))
            System.out.println("guid = " + j + " - is whitelisted");
        }
      }

      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {

      }
    }

    System.out.println("...........test ended.........");

  }
  */
}
