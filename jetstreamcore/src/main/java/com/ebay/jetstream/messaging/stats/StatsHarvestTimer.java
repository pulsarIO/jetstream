/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.stats;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.MessageService;

/**
 * @author shmurthy
 *
 * 
 */
public class StatsHarvestTimer extends TimerTask {

  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
    
  @Override
  public void run() {
    
    try {
      MessageService.getInstance().harvestStats();
    } catch (Throwable t) {
      LOGGER.error( "Exception \'" + t.getMessage() + "\' in StatsHarvestTimer");
      
    }
  }

}
