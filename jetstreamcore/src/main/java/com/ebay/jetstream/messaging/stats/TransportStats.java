/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.stats;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy
 *
 * Transport stats
 */
@ManagedResource
public class TransportStats implements XSerializable {

  private long m_totalMsgsSent;
  private long m_msgsSentPerSec;
  private long m_totalMsgsRcvd;
  private long m_msgsRcvdPerSec;
  private long m_totalMsgsDropped;
  private String m_context;
  private String m_protocol;

  public TransportStats() {
  }

  /**
   * @return the context
   */
  public String getContext() {
    return m_context;
  }

  /**
   * @return the msgsRcvdPerSec
   */
  public long getMsgsRcvdPerSec() {
    return m_msgsRcvdPerSec;
  }

  /**
   * @return the msgsSentPerSec
   */
  public long getMsgsSentPerSec() {
    return m_msgsSentPerSec;
  }

  /**
   * @return the protocol
   */
  public String getProtocol() {
    return m_protocol;
  }

  /**
   * @return the totalMsgsRcvd
   */
  public long getTotalMsgsRcvd() {
    return m_totalMsgsRcvd;
  }

  /**
   * @return the totalMsgsSent
   */
  public long getTotalMsgsSent() {
    return m_totalMsgsSent;
  }

  /**
   * @param context
   *            the context to set
   */
  public void setContext(String context) {
    m_context = context;
  }

  /**
   * @param msgsRcvdPerSec
   *            the msgsRcvdPerSec to set
   */
  public void setMsgsRcvdPerSec(long msgsRcvdPerSec) {
    m_msgsRcvdPerSec = msgsRcvdPerSec;
  }

  /**
   * @param msgsSentPerSec
   *            the msgsSentPerSec to set
   */
  public void setMsgsSentPerSec(long msgsSentPerSec) {
    m_msgsSentPerSec = msgsSentPerSec;
  }

  /**
   * @param protocol
   *            the protocol to set
   */
  public void setProtocol(String protocol) {
    m_protocol = protocol;
  }

  /**
   * @param totalMsgsRcvd
   *            the totalMsgsRcvd to set
   */
  public void setTotalMsgsRcvd(long totalMsgsRcvd) {
    m_totalMsgsRcvd = totalMsgsRcvd;
  }

  /**
   * @param totalMsgsSent
   *            the totalMsgsSent to set
   */
  public void setTotalMsgsSent(long totalMsgsSent) {
    m_totalMsgsSent = totalMsgsSent;
  }

  /**
   * @return the totalMsgsDropped
   */
  public long getTotalMsgsDropped() {
    return m_totalMsgsDropped;
  }

  /**
   * @param totalMsgsDropped the totalMsgsDropped to set
   */
  public void setTotalMsgsDropped(long totalMsgsDropped) {
    m_totalMsgsDropped = totalMsgsDropped;
  }
}
