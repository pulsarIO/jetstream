/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

/**
 * The EventException serves as the base class for all specialized exceptions that can be thrown by the event framework.
 * The primary enhancement is the ability to associate an event identifier of the causing event.
 * 
 * @author Dan Pritchett
 * 
 */
public class EventException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private EventId m_eventId;
  private String m_jetstreamErrorCode;

  public EventException() {
  }

  public EventException(EventId eventId) {
    super();
    m_eventId = eventId;
  }

  public EventException(String message) {
    super(message);
  }

  public EventException(String message, EventId eventId) {
    super(message);
    m_eventId = eventId;
  }

  public EventException(String message, String errorCode) {
    super(message);
    m_jetstreamErrorCode = errorCode;
  }

  public EventException(String message, Throwable cause) {
    super(message, cause);
  }

  public EventException(String message, Throwable cause, EventId eventId) {
    super(message, cause);
    m_eventId = eventId;
  }

  public EventException(Throwable cause) {
    super(cause);
  }

  public EventException(Throwable cause, EventId eventId) {
    super(cause);
    m_eventId = eventId;
  }

  public String getErrorCode() {
    return m_jetstreamErrorCode;
  }

  public EventId getEventId() {
    return m_eventId;
  }

  public void setEventId(EventId eventId) {
    m_eventId = eventId;
  }

  public void setJetstreamErrorCode(String jetstreamErrorCode) {
    m_jetstreamErrorCode = jetstreamErrorCode;
  }
}
