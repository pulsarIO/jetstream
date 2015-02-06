/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support.channel;

import com.ebay.jetstream.spring.context.support.AbstractApplicationEvent;

public final class PauseEvent extends AbstractApplicationEvent {
  private static final long serialVersionUID = 1L;

  public PauseEvent(Object source) {
    super(source);
  }
}
