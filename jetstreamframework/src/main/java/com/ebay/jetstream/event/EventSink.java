/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import org.springframework.beans.factory.NamedBean;

/**
 * An EventSink receives events. Events sent to the sink are dispositioned based on the policies of the sink.
 * 
 * @author Dan Pritchett
 * 
 */
public interface EventSink extends NamedBean {
  /**
   * Receive an event that is sent to this sink.
   * 
   * @param event
   *          The event being sent.
   * @throws EventException
   *           thrown if the event cannot be processed.
   */
  void sendEvent(JetstreamEvent event) throws EventException;
}
