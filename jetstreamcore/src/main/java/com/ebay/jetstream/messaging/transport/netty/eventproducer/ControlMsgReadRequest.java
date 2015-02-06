/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import com.ebay.jetstream.util.Request;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * This is a work Item triggering reading of the controlQueue.
 * An instance of this must be inserted in to dataQueue for every
 * message inserted in to control queue
 */

public class ControlMsgReadRequest extends Request {

  @Override
  public boolean execute() {
   
    return true;
  }

}
