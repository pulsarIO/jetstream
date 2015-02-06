/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.protocol;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;


/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          Consumer send this message to publisher to ACK receipt of this message.
 *          Consumer is expected to copy the sequenceId from the Jetstream message in to the ACK message before sending it.
 */

public class EventConsumerAck extends JetstreamMessage {

	private static final long serialVersionUID = 1L;
	private long m_sequenceId = -1;
	private static JetstreamTopic ackTopic = new JetstreamTopic("ack");
	
	
	public EventConsumerAck() {
		
		setTopic(ackTopic);
		
	}
	
	public EventConsumerAck(long sequenceId) {
		m_sequenceId = sequenceId;
		setTopic(ackTopic);
	}
	
	public long getSequenceId() {
		return m_sequenceId;
	}

	public void setSequenceId(long sequenceId) {
		this.m_sequenceId = sequenceId;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeLong(m_sequenceId);
		
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException {
		super.readExternal(in);
		m_sequenceId = in.readLong();
		
	}
	
}
