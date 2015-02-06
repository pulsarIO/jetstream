/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.notification;

import java.util.Date;

import org.springframework.context.ApplicationEvent;

import com.ebay.jetstream.xmlser.XSerializable;
/**
 * This event class is used by components to post own status and optionally request to send 
 * an alert to TEC server.
 * 
 * @author gjin
 *
 */
public class StatusApplicationEvent extends ApplicationEvent implements XSerializable{
	private String m_status = "";
	private boolean m_sendAlert = false;
	/**
	 * 
	 * @param source the source component
	 * @param status the free-format status such as "init", "ok"...
	 */
	public StatusApplicationEvent(Object source, String status) {
		super(source);
		m_status = status;
	}
	/**
	 * 
	 * @param source the source component
	 * @param status the free-format status such as "init", "ok"...
	 * @param sendAlert if it is true, an alert can be sent to TEC alert server
	 *                  
	 */
	public StatusApplicationEvent(Object source, String status, boolean sendAlert) {
		super(source);
		m_status = status;
		m_sendAlert = sendAlert;
	}
	public String getStatus() {
		return m_status;
	}
	public boolean isSendAlert() {
		return m_sendAlert;
	}
	
	public Date getDateTime(){
		return new Date(getTimestamp());
	}
	
}
