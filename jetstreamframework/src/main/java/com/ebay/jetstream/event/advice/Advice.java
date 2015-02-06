/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.advice;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.RetryEventCode;

public interface Advice {
  public void abandon(JetstreamEvent event, int reasonCode, String reason);

  public void retry(JetstreamEvent event, RetryEventCode reasonCode, String reason);

  public void success(JetstreamEvent event);
  
  public void stopReplay();
  
  public void startReplay();
}
