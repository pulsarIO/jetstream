/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import java.util.Collection;

public interface BatchEventSink extends EventSink {
  void sendEvents(Collection<JetstreamEvent> events, EventMetaInfo meta) throws EventException;
}
