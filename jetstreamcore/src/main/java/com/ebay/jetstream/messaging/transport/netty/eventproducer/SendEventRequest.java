/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.util.Request;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * This class is the work item corresponding to requests to be sent to MINA
 * 
 * 
 */
public class SendEventRequest extends Request {

  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
  
   	private EventProducer m_eventproducer = null;
	private JetstreamMessage m_msg = null;
	
	public SendEventRequest() {}
	
	/**
	 * @param tsm
	 * @param msg
	 */
	public SendEventRequest(EventProducer tsm, JetstreamMessage msg)
	{
		
		m_eventproducer = tsm;
		m_msg = msg;
		
	}
	
	/* (non-Javadoc)
	 * @see com.ebay.jetstream.util.Request#execute()
	 */
	@Override
	public boolean execute() {
				
		try {
			m_eventproducer.dispatchDownStream(m_msg);
		} catch (Throwable t) {
			
            String msg = "Caught exception while executing SendEventRequest - " + t.getMessage();
            LOGGER.error( msg);
        }
		return true;
	}

	/**
	 * @return the eventproducer
	 */
	public EventProducer getEventproducer() {
		return m_eventproducer;
	}

	/**
	 * @param eventproducer the eventproducer to set
	 */
	public void setEventproducer(EventProducer eventproducer) {
		m_eventproducer = eventproducer;
	}

	/**
	 * @return the msg
	 */
	public JetstreamMessage getMsg() {
		return m_msg;
	}

	/**
	 * @param msg the msg to set
	 */
	public void setMsg(JetstreamMessage msg) {
		m_msg = msg;
	}

}
