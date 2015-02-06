/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel;

import com.ebay.jetstream.event.EventException;

public interface ChannelOperations {
  /**
   * Close the channel. Once closed can be sent. Depending upon the state of the channel at time of close, more may be
   * received until the pending queue has been drained. Close will flush before shutting down the channel.
   * 
   * @throws EventException
   */
  void close() throws EventException;

  /**
   * Flush the pending outbound events from the channel. This operation will immediately deliver any events being
   * buffered. Note that it may be a no-op on channels depending upon the delivery policy of the channel.
   * 
   * @throws EventException
   */
  void flush() throws EventException;

  /**
   * Return the address on this side of the channel connection.
   * 
   * @return local address
   */
  ChannelAddress getAddress();

  void open() throws EventException;

}
