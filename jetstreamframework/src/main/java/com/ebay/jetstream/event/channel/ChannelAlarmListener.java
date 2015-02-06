/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel;

/**
 * The Channel state listener receives an event when a channel is ready for communication. This occurs either when a
 * channel connect is complete or a new inbound channel session is created.
 * 
 *
 * 
 */
public interface ChannelAlarmListener {
  void alarm(ChannelAlarm alarm);
}
