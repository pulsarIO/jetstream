/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.util.Request;

/**
 * An implementation of a request thread pattern
 *  *
 * @author shmurthy@ebay.com
 * @version 1.0
 */

public class MessageServiceRequest extends Request {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	private JetstreamMessage m_tm;

	/**
	 * 
	 */
	public MessageServiceRequest() {
	}

	/**
	 * @param tm
	 */
	public MessageServiceRequest(JetstreamMessage tm) {
		m_tm = tm;
	}

	/* (non-Javadoc)
	 * @see com.ebay.jetstream.util.Request#execute()
	 */
	public boolean execute() {

        try {
          MessageService.getInstance().dispatch(m_tm);
        } catch (Throwable t) {
          String msg = "Caught exception while executing MessageServiceRequest - " + t.getMessage();
          LOGGER.error( msg);
        }
        
        return true;
	}

}
