/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.protocol;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This is the advertisement that an event consumer will send out. It is consumed by Event producers to
 *          discover consumers
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={"SBSC_USE_STRINGBUFFER_CONCATENATION", "DM_STRING_VOID_CTOR"})	
public class EventConsumerAdvertisement extends JetstreamMessage implements
		XSerializable {

	private static final long serialVersionUID = 1L;
	private static final byte VERSION_1 = 1;
	private static final byte VERSION_2 = 2;
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	private String m_hostName = "";
	private int m_listenTcpPort = -1;
	private String m_context = "";
	private long m_primaryAffinityKey = -1;
	private int m_poolSize = 0;
	private int m_weight = 10;
	private boolean m_isCompressionEnabled = false;
	private boolean m_isKryoSerializationEnabled = false;
	private boolean m_reserved = true;
	private long m_timeStamp;
	private long m_consumerId;
	private String m_hostPortStr;

	/**
	 * @return
	 */
	public long getConsumerId() {
		return m_consumerId;
	}

	/**
	 * @param m_consumerId
	 */
	public void setConsumerId(long m_consumerId) {
		this.m_consumerId = m_consumerId;
	}

	private List<JetstreamTopic> m_interestedTopics = new LinkedList<JetstreamTopic>();

	/**
   * 
   */
	public EventConsumerAdvertisement() {
	}

	/**
	 * @param port
	 * @param hostName
	 */
	public EventConsumerAdvertisement(int port, String hostName) {
		m_listenTcpPort = port;
		m_hostName = hostName;
	}

	/**
	 * @param port
	 * @param hostName
	 * @param context
	 * @param interestedTopics
	 */
	public EventConsumerAdvertisement(int port, String hostName,
			String context, List<JetstreamTopic> interestedTopics) {
		m_listenTcpPort = port;
		m_interestedTopics = interestedTopics;
		m_hostName = hostName;
		m_context = context;
		setPriority(JetstreamMessage.HI_PRIORITY);
	}

	/**
	 * @param topic
	 * @return
	 */
	public boolean containsTopic(JetstreamTopic topic) {
		return m_interestedTopics.contains(topic);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj == this)
			return true;

		if (obj == null)
			return false;

		if (!(obj instanceof EventConsumerAdvertisement))
			return false;

		EventConsumerAdvertisement eca = (EventConsumerAdvertisement) obj;

		if (!m_hostName.equals(eca.m_hostName))
			return false;
		if (!m_context.equals(eca.m_context))
			return false; // BUG fix
		if (m_listenTcpPort != eca.m_listenTcpPort)
			return false;
		if (m_primaryAffinityKey != eca.m_primaryAffinityKey)
			return false;
		if (m_weight != eca.m_weight)
			return false;
		if (m_isCompressionEnabled != eca.m_isCompressionEnabled)
			return false;
		if (m_isKryoSerializationEnabled != eca.m_isKryoSerializationEnabled)
			return false;
		if (m_poolSize != eca.m_poolSize)
			return false;		
		if (m_consumerId != eca.m_consumerId)
			return false;
		
		Iterator<JetstreamTopic> itr = m_interestedTopics.iterator();

		if (m_interestedTopics.size() != eca.m_interestedTopics.size())
			return false;
		while (itr.hasNext()) {
			if (!eca.m_interestedTopics.contains(itr.next()))
				return false;
		}

		return true;
	}
	
	
	public int hashCode() {
		return Integer.valueOf(m_poolSize).hashCode() +
			   Integer.valueOf(m_weight).hashCode() +
			   Integer.valueOf(m_listenTcpPort).hashCode() +
			   m_context.hashCode() +
			   m_hostName.hashCode();
	}

	
	/**
	 * @return the context
	 */
	public String getContext() {
		return m_context;
	}

	/**
	 * @return the hostName
	 */
	public String getHostName() {
		return m_hostName;
	}

	/**
	 * @return the interestedTopics
	 */
	public List<JetstreamTopic> getInterestedTopics() {
		return m_interestedTopics;
	}

	/**
	 * @return the isBackupAffinityNode
	 */
	public boolean isCompressionEnabled() {
		return m_isCompressionEnabled;
	}
	
	public boolean getCompressionEnabled() {
		return m_isCompressionEnabled;
	}

	/**
	 * @return the listenTcpPort
	 */
	public int getListenTcpPort() {
		return m_listenTcpPort;
	}

	/**
	 * @return the poolSize
	 */
	public int getPoolSize() {
		return m_poolSize;
	}

	/**
	 * @return the primaryAffinitKey
	 */
	public long getPrimaryAffinityKey() {
		return m_primaryAffinityKey;
	}

	/**
	 * @return the timeStamp
	 */
	public Date getTimeStamp() {
		return new Date(m_timeStamp);
	}

	/**
	 * @return the weight
	 */
	public int getWeight() {
		return m_weight;
	}

	/**
	 * @return
	 */
	public boolean isAssignedPrimaryAffinityKey() {

		if (m_primaryAffinityKey != -1)
			return true;
		else
			return false;
	}

	
	/**
	 * @return the isSecondaryBackup
	 */
	public boolean isKryoSerializationEnabled() {
		return m_isKryoSerializationEnabled;
	}

	/**
	 * @param primaryAffinitKey
	 *            the primaryAffinitKey to set
	 */
	public boolean isPrimaryAffinitKey(Long primaryAffinityKey) {
		return m_primaryAffinityKey == primaryAffinityKey.longValue();
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

		m_listenTcpPort = in.readInt();

		m_weight = in.readInt();

		m_poolSize = in.readInt();

		m_primaryAffinityKey = in.readLong();

		try {
			m_hostName = (String) in.readObject();
			m_context = (String) in.readObject();
		} catch (ClassNotFoundException e1) {

			String msg = "Unmarshallign exception - " + e1.getMessage();
			LOGGER.error( msg);
			return;
		}

		m_consumerId = in.readLong();

		int numTopics = in.readInt();

		m_interestedTopics = new LinkedList<JetstreamTopic>();

		for (int i = 0; i < numTopics; i++) {
			try {
				JetstreamTopic topic = (JetstreamTopic) in.readObject();
				m_interestedTopics.add(topic);
			} catch (ClassNotFoundException e) {
			
				LOGGER.error( "Unmarshallign exception - " + e.getLocalizedMessage());
			}
		}

		m_isCompressionEnabled = in.readBoolean();

		m_isKryoSerializationEnabled = in.readBoolean();

		m_reserved = in.readBoolean();

		// added this for 1.1

		if (getVersion() > VERSION_1)
			m_timeStamp = in.readLong();
		
		StringBuffer buf = new StringBuffer();

		buf.append(getHostName());
		buf.append("-");
		buf.append(getListenTcpPort());

		m_hostPortStr = buf.toString();
	}

	/**
	 * @param topic
	 */
	public void registerInterest(JetstreamTopic topic) {
		m_interestedTopics.add(topic);
	}

	/**
	 * @param topic
	 */
	public void removeTopic(JetstreamTopic topic) {
		if (m_interestedTopics.contains(topic))
			m_interestedTopics.remove(topic);
	}

	/**
	 * @param belongsToAffinityPool
	 *            the belongsToAffinityPool to set
	 */
	public void setBelongsToAffinityPool(boolean belongsToAffinityPool) {
		m_reserved = belongsToAffinityPool;
	}

	/**
	 * @param context
	 *            the context to set
	 */
	public void setContext(String context) {
		m_context = context;
	}

	/**
	 * @param isPrimaryBackup
	 *            the isPrimaryBackup to set
	 */
	public void setCompressionEnabled(boolean isCompressionEnabled) {
		m_isCompressionEnabled = isCompressionEnabled;
	}

	/**
	 * @param isSecondaryBackup
	 *            the isSecondaryBackup to set
	 */
	public void setKryoSerializationEnabled(boolean enableKryoSerialization) {
		m_isKryoSerializationEnabled = enableKryoSerialization;
		
	}

	/**
	 * @param hostName
	 *            the hostName to set
	 */
	public void setHostName(String hostName) {
		m_hostName = hostName;
	}

	/**
	 * @param interestedTopics
	 *            the interestedTopics to set
	 */
	public void setInterestedTopics(List<JetstreamTopic> interestedTopics) {
		m_interestedTopics = interestedTopics;
	}

	/**
	 * @param listenTcpPort
	 *            the listenTcpPort to set
	 */
	public void setListenTcpPort(int listenTcpPort) {
		m_listenTcpPort = listenTcpPort;
	}

	/**
	 * @param poolSize
	 *            the poolSize to set
	 */
	public void setPoolSize(int poolSize) {
		m_poolSize = poolSize;
	}

	
	/**
	 * @param timeStamp
	 *            the timeStamp to set
	 */
	public void setTimeStamp(long timeStamp) {
		m_timeStamp = timeStamp;
	}

	/**
	 * @param weight
	 *            the weight to set
	 */
	public void setWeight(int weight) {
		m_weight = weight;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.messaging.JetstreamMessage#toString()
	 */
	@Override
	public String toString() {
		String strValue = "\nHost Name = ";
		strValue += m_hostName;
		strValue += "\nweight = ";
		strValue += m_weight;
		strValue += "\nport = ";
		strValue += Integer.valueOf(m_listenTcpPort).toString();
		strValue += "\nconsumerId = ";
		strValue += m_consumerId;
		strValue += "\npool size = ";
		strValue += Integer.valueOf(m_poolSize).toString();
		strValue += "\nprimary affinity key = ";
		strValue += Long.valueOf(m_primaryAffinityKey).toString();
		strValue += "\nJetstream Context = ";
		strValue += m_context;
		strValue += "\ntimestamp = ";
		strValue += new Date(m_timeStamp).toString();
		strValue += "\nInterested Topics : \n";

		Iterator<JetstreamTopic> itr = m_interestedTopics.iterator();

		while (itr.hasNext()) {
			strValue += itr.next().getTopicName();
			strValue += "\n";

		}

		strValue += "\nisCompressionEnabled = ";
		strValue += m_isCompressionEnabled;
		strValue += "\nisKryoSerializationEnabled = ";
		strValue += m_isKryoSerializationEnabled;
		strValue += "\nisbelongsToAffinityPool = ";
		strValue += m_reserved;
		strValue += "\nhostAndPort = ";
		strValue += getHostAndPort();

		return strValue;

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

		setVersion(VERSION_2);

		super.writeExternal(out);

		out.writeInt(m_listenTcpPort);

		out.writeInt(m_weight);

		out.writeInt(m_poolSize);

		out.writeLong(m_primaryAffinityKey);

		out.writeObject(m_hostName);

		out.writeObject(m_context);

		out.writeLong(m_consumerId);

		out.writeInt(m_interestedTopics.size());

		Iterator<JetstreamTopic> itr = m_interestedTopics.iterator();

		while (itr.hasNext()) {
			out.writeObject(itr.next());
		}

		out.writeBoolean(m_isCompressionEnabled);

		out.writeBoolean(m_isKryoSerializationEnabled);

		out.writeBoolean(m_reserved);

		out.writeLong(m_timeStamp);

	}

	/**
	 * @return
	 */
	public String getHostAndPort() {
				
		return m_hostPortStr;
		
	}

}
