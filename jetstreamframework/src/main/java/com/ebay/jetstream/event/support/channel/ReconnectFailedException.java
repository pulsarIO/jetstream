/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support.channel;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.channel.ChannelAddress;

public class ReconnectFailedException extends EventException {
  private static final long serialVersionUID = 1L;
  private final ChannelAddress m_address;

  public ReconnectFailedException(String message, ChannelAddress address) {
    super(message);
    m_address = address;
  }

  public ChannelAddress getAddress() {
    return m_address;
  }

}
