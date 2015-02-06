/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.math;

import com.google.common.util.concurrent.AtomicDouble;

/*
 * @author shmurthy@ebay.com
 * @version 1.0
 *
 * "EWMA" => "Exponentially Weighted Moving Average"
 * Sn = alpha * Yn-1 + (1-alpha) * Sn-1
 */

public class EWMA {

  private boolean m_firstTime = true;
  private double m_prevAvg; // previous avg
  private double m_currentAvg; // current avg
  private double m_lastSample;
  private final double m_smoothingFactor; // smoothing factor
  private AtomicDouble m_atomicCurAvg = new AtomicDouble(0);

  public EWMA(int samples) {
    m_smoothingFactor = (double) 2 / samples; // calculate smoothing factor
  }

  /**
   * @param sample
   */
  public synchronized void add(double sample) {
    if (m_firstTime) {
      if (sample < 1.0) {
        return;
      }
      m_currentAvg = m_prevAvg = sample;
      m_atomicCurAvg.set(m_currentAvg);
      m_firstTime = false;
    }
    else {
      m_currentAvg = m_smoothingFactor * m_lastSample + (1 - m_smoothingFactor) * m_prevAvg;
      m_prevAvg = m_currentAvg;
      m_atomicCurAvg.set(m_currentAvg);
    }
    m_lastSample = sample;
  }

  /**
   * @return
   */
  public double getAverage() {
    return m_atomicCurAvg.get();
  }

}
