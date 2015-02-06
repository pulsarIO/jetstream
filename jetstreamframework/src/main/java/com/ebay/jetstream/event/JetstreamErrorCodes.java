/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;


public enum JetstreamErrorCodes {

  EVENT_SINK_PAUSED("E3001", "One or more event sinks are paused"),

  EVENT_SINKS_PAUSED("E3002", "All the event sinks are paused");

  private String m_errorCode;

  private String m_message;

  JetstreamErrorCodes(String errorCode, String message) {
    m_errorCode = errorCode;
    m_message = message;
  }

  public String getErrorCode() {
    return m_errorCode;
  }

  public String getMessage() {
    return m_message;
  }

  @Override
  public String toString() {
    return m_errorCode;
  }

}
