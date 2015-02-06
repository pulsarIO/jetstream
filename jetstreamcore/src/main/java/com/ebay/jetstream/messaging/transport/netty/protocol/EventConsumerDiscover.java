/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.protocol;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * This is a discover message sent out by event producers and is consumed by
 * event consumers. The event producers send this message out to discover event consumers. The
 * event consumers will send out an advertisement in response to this message
 * 
 */

public class EventConsumerDiscover extends JetstreamMessage {

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private String m_context = "";

	private String m_hostName = "";

	/**
	 * 
	 */
	public EventConsumerDiscover() {
	}

	/**
	 * @param context
	 * @param hostName
	 */
	public EventConsumerDiscover(String context, String hostName) {
		m_context = context;
		m_hostName = hostName;
		setPriority(JetstreamMessage.HI_PRIORITY);
	}

	/**
	 * @return the context
	 */
	public String getContext() {
		return m_context;
	}

	/**
	 * @param context
	 *            the context to set
	 */
	public void setContext(String context) {
		m_context = context;
	}

	/**
	 * @return the hostName
	 */
	public String getHostName() {
		return m_hostName;
	}

	/**
	 * @param hostName
	 *            the hostName to set
	 */
	public void setHostName(String hostName) {
		m_hostName = hostName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessage#writeExternal(java.io.ObjectOutput
	 * )
	 */
	public void writeExternal(ObjectOutput out) throws IOException {

		super.writeExternal(out);

		out.writeObject(m_hostName);

		out.writeObject(m_context);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessage#readExternal(java.io.ObjectInput
	 * )
	 */
	public void readExternal(ObjectInput in) throws IOException {

		super.readExternal(in);

		try {
			m_hostName = (String) in.readObject();
			m_context = (String) in.readObject();
		} catch (ClassNotFoundException e1) {

			String msg = "Unmarshalling error in EventConsumerDiscover - " + e1.getMessage();

			LOGGER.warn( msg);

			return;
		}

	}

	

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.messaging.JetstreamMessage#toString()
	 */
	public String toString() {
		String strValue = "\nHost Name = ";
		strValue += m_hostName;
		strValue += "\nJetstream Context = ";
		strValue += m_context;

		return strValue;

	}

}
