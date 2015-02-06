/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.util.Request;

/**
 * An implementation of a request to resume the traffic on Upstream Dispatcher Queue
 *  *
 * @author rmuthupandian
 * @version 1.0
 */

public class ResumeTrafficRequest extends Request {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
   
	public ResumeTrafficRequest() {
		
	}

	
	/* (non-Javadoc)
	 * @see com.ebay.jetstream.util.Request#execute()
	 */
	public boolean execute() {

        try {
        	MessageService.getInstance().resumeTraffic();
        } catch (Throwable t) {
          String msg = "Caught exception while executing ResumeTrafficRequest - " + t.getMessage();
          LOGGER.error( msg);
        }
        
        return true;
	}

}