/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import com.ebay.jetstream.config.ConfigChangeMessage;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

public class MessageServiceTestApplicationListener implements IMessageListener {

  public void onMessage(JetstreamMessage m) {
     
    System.out.println("Receving..."); //KEEPME
    
    if (m instanceof ConfigChangeMessage) {
          
      MessageServiceTestApplication.setMessageReceivedCount();
    }


    
  }

}
