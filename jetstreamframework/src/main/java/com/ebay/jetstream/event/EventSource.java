/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import java.util.Collection;

/**
 * An EventSource creates events and delivers them to the provided sinks. EventSinks are registered by type or can be
 * registered to handle all events with the provided constant.
 * 
 * @author Dan Pritchett
 * 
 */
public interface EventSource {
  /**
   * Add an event sink to this event source.
   * 
   * @param target
   *            The sink that will receive the events.
   */
  void addEventSink(EventSink target);

  /**
   * Gets the complete collection of event sinks for this event source.
   * 
   * @return the collection of event sinks connected to the event source.
   */
  Collection<EventSink> getEventSinks();

  /**
   * Remove an event sink from this event source.
   * 
   * @param target
   *            The target to remove
   */
  void removeEventSink(EventSink target);

  /**
   * Sets the complete collection of event sinks for this source.
   * 
   * @param sinks
   *            the collection of event sinks to set for this source.
   */
  void setEventSinks(Collection<EventSink> sinks);
  

}
