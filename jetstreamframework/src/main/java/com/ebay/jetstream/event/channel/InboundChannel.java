/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.EventSource;
import com.ebay.jetstream.event.Monitorable;

public interface InboundChannel extends EventSource, ChannelOperations, Monitorable {

  /**
   * Pause the receipt of messages. Note that messages may be delivered after a pause. Calling pause on a paused channel
   * should be ignored. This operation may be a no-op depending upon the channel semantics.
   * 
   * @throws EventException
   */
  void pause() throws EventException;

  /**
   * Resume the receipt of messages. Calling resume on an active channel should be ignored. This operation may be a
   * no-op depending upon the channel semantics.
   * 
   * @throws EventException
   */
  void resume() throws EventException;

}
