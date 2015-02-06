/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.counter;

import java.util.Timer;
import java.util.TimerTask;

import com.ebay.jetstream.util.math.EWMA;

/**
 * @author shmurthy
 * 
 */
public class LongEWMACounter {

  public static class EWMAUpdateTask extends TimerTask {

    private final LongEWMACounter m_ewmaCounter;

    public EWMAUpdateTask(LongEWMACounter ewmaCounter) {
      m_ewmaCounter = ewmaCounter;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.TimerTask#run()
     */
    @Override
    public void run() {
      m_ewmaCounter.updateEWMA();

    }

  }

  private final static int WITH_ONE_SEC_DELAY = 1000;
  private final static int ONE_SECOND_PERIOD = 1000;

  private final LongCounter m_count = new LongCounter();

  private final EWMA m_ewma;
  private final Timer m_timer;
  private final EWMAUpdateTask m_timertask;

  public LongEWMACounter(int duration, Timer timer) {
    m_ewma = new EWMA(duration);
    m_timer = timer;
    m_timertask = new EWMAUpdateTask(this);
    m_timer.scheduleAtFixedRate(m_timertask, WITH_ONE_SEC_DELAY, ONE_SECOND_PERIOD);

  }
  
  public LongEWMACounter(int duration, Timer timer, int period) {
	    m_ewma = new EWMA(duration);
	    m_timer = timer;
	    m_timertask = new EWMAUpdateTask(this);
	    m_timer.scheduleAtFixedRate(m_timertask, period, period);

  }

  public void add(long value) {
    m_count.addAndGet(value);
  }

  public void destroy() {
    m_timertask.cancel();
  }

  // this method is expected to be invoked every sec

  public long get() {
    return (long) m_ewma.getAverage();
  }

  public void increment() {
    m_count.increment();

  }

  /**
   * 
   */
  public void reset() {
    m_count.getAndReset();
  }

  private void updateEWMA() {

    m_ewma.add(m_count.getAndReset());

  }

}
