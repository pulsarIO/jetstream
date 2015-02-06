/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel;

import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.event.Monitorable;

public interface OutboundChannel extends EventSink, ChannelOperations, Monitorable {
  ChannelAlarmListener getAlarmListener();

  void setAlarmListener(ChannelAlarmListener alarm);

}
