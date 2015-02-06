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

import com.ebay.jetstream.messaging.topic.JetstreamTopic;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version
 */

public class AdvisoryMessage extends JetstreamMessage {

	public enum AdvisoryCode {
		MESSAGE_LOST, STOP_SENDING, RESUME_SENDING, RESEND_MESSAGE
	}

	private JetstreamMessage m_undeliveredMsg;

	private static final long serialVersionUID = 1L;

	private JetstreamTopic m_topic;

	private AdvisoryCode m_code;

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	/**
	 * 
	 */
	public AdvisoryMessage() {
	}

	/**
	 * @param topic
	 * @param code
	 */
	public AdvisoryMessage(JetstreamTopic topic, AdvisoryCode code) {
		m_code = code;
		m_topic = topic;
		JetstreamTopic advisoryTopic = new JetstreamTopic("JetstreamAdvisory");
		super.setTopic(advisoryTopic);
	}

	/**
	 * @return
	 */
	public AdvisoryCode getAdvisoryCode() {
		return m_code;
	}

	/**
	 * @return
	 */
	public JetstreamTopic getAdvisoryTopic() {
		return m_topic;
	}

	/**
	 * @return the undeliveredMsg
	 */
	public JetstreamMessage getUndeliveredMsg() {
		return m_undeliveredMsg;
	}

	/**
	 * @return
	 */
	public boolean isResendMessage() {
		return m_code == AdvisoryCode.RESEND_MESSAGE;
	}

	/**
	 * @return
	 */
	public boolean isResumeSending() {
		return m_code == AdvisoryCode.RESUME_SENDING;
	}

	/**
	 * @return
	 */
	public boolean isStopSending() {
		return m_code == AdvisoryCode.STOP_SENDING;
	}

	

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessage#readExternal(java.io.ObjectInput
	 * )
	 */
	@Override
	public void readExternal(ObjectInput in) throws IOException {
		
		super.readExternal(in);

		m_topic = new JetstreamTopic();
		try {
			m_topic = (JetstreamTopic) in.readObject();
			m_code = (AdvisoryCode) in.readObject();
		} catch (Exception e) {

			if (LOGGER.isWarnEnabled()) {
				String message = "Unmarshalling Error - ";
				message += e.getMessage();

				LOGGER.warn( message);

			}
		}
	}

	/**
	 * @param code
	 */
	public void setAdvisoryCode(AdvisoryCode code) {

		m_code = code;
	}

	/**
	 * @param topic
	 */
	public void setAdvisoryTopic(JetstreamTopic topic) {
		m_topic = topic;

	}

	/**
	 * @param undeliveredMsg
	 *            the undeliveredMsg to set
	 */
	public void setUndeliveredMsg(JetstreamMessage undeliveredMsg) {
		m_undeliveredMsg = undeliveredMsg;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.messaging.JetstreamMessage#toString()
	 */
	@Override
	public String toString() {

		String advStr = "Internal Advisory - \n";
		advStr += "context - ";
		advStr += m_topic.getTopicName();
		advStr += "\n advisory code - ";
		advStr += m_code;

		return advStr;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessage#writeExternal(java.io.ObjectOutput
	 * )
	 */
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {

		super.writeExternal(out);
		out.writeObject(m_topic);
		out.writeObject(m_code);

	}

}