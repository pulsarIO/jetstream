/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.messagetype;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author shmurthy
 *
 * ActivityChangeEvent is delivered by the messaging framework when a subscriber
 * subscribes to the ft.group context. It is delivered only when the subscriber's instance
 * of the messaging service changes state to active from inactive. 
 *  
 */

public class ActivityChangeMessage extends JetstreamMessage {

	public enum ActivityStatus {
		Active, Inactive
	};

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	private ActivityStatus m_oldActivityStatus;
	private ActivityStatus m_newActivityStatus;

	/**
	 * 
	 */
	public ActivityChangeMessage() {
	}

	/**
	 * @param oldActivity
	 * @param newActivity
	 */
	public ActivityChangeMessage(ActivityStatus oldActivity,
			ActivityStatus newActivity) {
		m_oldActivityStatus = oldActivity;
		m_newActivityStatus = newActivity;

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
		out.writeObject(this.m_oldActivityStatus);
		out.writeObject(this.m_newActivityStatus);

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
			this.m_oldActivityStatus = (ActivityStatus) in.readObject();
			this.m_newActivityStatus = (ActivityStatus) in.readObject();
		} catch (Exception e) {

			if (LOGGER.isWarnEnabled()) {
				String message = "Unmarshalling Error - ";
				message += e.getMessage();

				LOGGER.warn( message);

			}
		}
	}

	/**
	 * @return the newActivityStatus
	 */
	public ActivityStatus getNewActivityStatus() {
		return m_newActivityStatus;
	}

	/**
	 * @param newActivityStatus
	 *            the newActivityStatus to set
	 */
	public void setNewActivityStatus(ActivityStatus newActivityStatus) {
		m_newActivityStatus = newActivityStatus;
	}

	/**
	 * @return the oldActivityStatus
	 */
	public ActivityStatus getOldActivityStatus() {
		return m_oldActivityStatus;
	}

	/**
	 * @param oldActivityStatus
	 *            the oldActivityStatus to set
	 */
	public void setOldActivityStatus(ActivityStatus oldActivityStatus) {
		m_oldActivityStatus = oldActivityStatus;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.messaging.JetstreamMessage#toString()
	 */
	public String toString() {
		String optStr = "current activity = " + m_newActivityStatus;

		return optStr;
	}

	
}
