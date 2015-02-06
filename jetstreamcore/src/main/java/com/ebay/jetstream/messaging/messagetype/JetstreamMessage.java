/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.messagetype;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.xmlser.Hidden;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version
 */

public class JetstreamMessage implements Externalizable {

  private static final long serialVersionUID = 1L;
  public static final byte HI_PRIORITY = 0;
  public static final byte LOW_PRIORITY = 1;
  public static final byte INTERNAL_MSG_PRIORITY = 2; // reserved for internal messages only
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.messagetype");

  static InetAddress m_localinetaddr = null;

  static {
    try {
      m_localinetaddr = java.net.InetAddress.getLocalHost();
    }
    catch (Exception e) {
      LOGGER.error( "unable to find localhost" + e.getLocalizedMessage());
    }
  }

  private byte m_headerVersion = 1;

  private byte m_version = 1;

  private JetstreamTopic m_topic;

  private byte m_priority;

  private byte[] m_msgOrigAddr;

  private long m_sequenceId = -1;

  private long m_guid; // this is to distinguish the message sent from

  private boolean m_broadcastMessage = false;

  private Object m_affinityKey = -1;

  private Object m_dispatchId;

  private boolean m_requiresAffinity = false;

  private int m_TTL = 4; // we will retry sending this message a
  // a max of 4 times
  private byte m_spare1;
  private byte m_spare2;
  private byte m_spare3;

  /**
	 * 
	 */
  public JetstreamMessage() {
    m_priority = 1;
    m_msgOrigAddr = m_localinetaddr.getAddress();

  }

  /**
   * @param version
   * @param priority
   * @param topic
   */
  public JetstreamMessage(byte version, byte priority, JetstreamTopic topic) {
    m_version = version;
    m_priority = priority;
    m_topic = topic;

    m_msgOrigAddr = m_localinetaddr.getAddress();

  }

  /**
   * @return the tTL
   */
  public int decTTL() {

    if (m_TTL > 0)
      return m_TTL--;
    else
      return m_TTL;
  }

  /**
   * @return the affinityKey
   */

  @Hidden
  public Object getAffinityKey() {
    return m_affinityKey;
  }

  /**
   * @return the dispatchId
   */

  @Hidden
  public Object getDispatchId() {
    return m_dispatchId;
  }

  /**
   * @return
   */
  public long getGuid() {
    return m_guid;
  }

  /**
   * @return
   */
  @Hidden
  public byte[] getMsgOrigination() {
    return m_msgOrigAddr;
  }

  /**
   * @return
   */
  public int getPriority() {
    return m_priority;
  }

  /**
   * @return
   */
  public long getSequenceId() {
    return m_sequenceId;
  }

  /**
   * @return
   */
  public JetstreamTopic getTopic() {
    return m_topic;
  }

  /**
   * @return
   */
  public int getVersion() {
    return m_version;
  }

  /**
   * @return
   */
  public boolean isMsgFromSameStation() {

    byte[] addr;

    addr = m_localinetaddr.getAddress();

    return Arrays.equals(m_msgOrigAddr, addr);
  }

  /**
   * @return the requiresAffinity
   */
  public boolean requiresAffinity() {
    return m_requiresAffinity;
  }

  /**
   * @return the sprayMessage
   */
  public boolean broadcast() {
    return m_broadcastMessage;
  }

  
  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  public void readExternal(ObjectInput in) throws IOException {

    m_headerVersion = in.readByte();
    m_version = in.readByte();
    m_priority = in.readByte();
    m_sequenceId = in.readLong();
    m_guid = in.readLong();
    int len = in.readByte();
    m_msgOrigAddr = new byte[len];
    in.readFully(m_msgOrigAddr);

    m_spare1 = in.readByte();
    m_spare2 = in.readByte();
    m_spare3 = in.readByte();

    m_topic = new JetstreamTopic();
    m_topic.readExternal(in);

  }

  /**
   * @param affinityKey
   *          the affinityKey to set
   */
  public void setAffinityKey(Object affinityKey) {
    m_affinityKey = affinityKey;
    setRequiresAffinity(true);
  }

  /**
   * @param dispatchId
   *          the dispatchId to set
   */

  public void setDispatchId(Object dispatchId) {
    m_dispatchId = dispatchId;
  }

  /**
   * @param guid
   */
  public void setGuid(long guid) {
    m_guid = guid;
  }

  /**
   * @param addr
   */
  public void setMsgOrigination(byte[] addr) {
    m_msgOrigAddr = addr;
  }

  /**
   * @param priority
   */
  public void setPriority(byte priority) {
    m_priority = priority;
  }

  /**
   * @param requiresAffinity
   *          the requiresAffinity to set
   */
  public void setRequiresAffinity(boolean requiresAffinity) {
    m_requiresAffinity = requiresAffinity;
  }

  /**
   * @param sequenceid
   */
  public void setSequenceId(long sequenceid) {
    m_sequenceId = sequenceid;
  }

  /**
   * @param sprayMessage
   *          the sprayMessage to set
   */
  public void setBroadcastMessage(boolean sprayMessage) {
    m_broadcastMessage = sprayMessage;
  }

  /**
   * @param topic
   */
  public void setTopic(JetstreamTopic topic) {
    m_topic = topic;
  }

  /**
   * @param ttl
   *          the tTL to set
   */
  public void setTTL(int ttl) {
    m_TTL = ttl;
  }

  /**
   * @param version
   */
  public void setVersion(byte version) {
    m_version = version;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    String msgStr = "Topic Name = ";
    if (m_topic != null) {
      msgStr += m_topic.getTopicName();
    }
    msgStr += "\n";
    msgStr += "Message Header Version = ";
    msgStr += m_headerVersion;
    msgStr += "Message Version = ";
    msgStr += m_version;
    msgStr += "Message Priority = ";
    msgStr += m_priority;
    msgStr += "Message Sequence Id = ";
    msgStr += m_sequenceId;
    msgStr += "Message GUID = ";
    msgStr += m_guid;
    msgStr += "Message Affinity Key = ";
    msgStr += m_affinityKey;

    return msgStr;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  public void writeExternal(ObjectOutput out) throws IOException {

    out.writeByte(m_headerVersion);
    out.writeByte(m_version);
    out.writeByte(m_priority);
    out.writeLong(m_sequenceId);
    out.writeLong(m_guid);
    out.writeByte(m_msgOrigAddr.length);
    out.write(m_msgOrigAddr);
    out.writeByte(m_spare1);
    out.writeByte(m_spare2);
    out.writeByte(m_spare3);

    m_topic.writeExternal(out);

  }

    public void writeKryo(Kryo kryo, Output out) {
        out.writeByte(m_headerVersion);
        out.writeByte(m_version);
        out.writeByte(m_priority);
        out.writeLong(m_sequenceId, true);
        out.writeLong(m_guid, true);
        out.writeByte(m_msgOrigAddr.length);
        out.write(m_msgOrigAddr);
        out.writeByte(m_spare1);
        out.writeByte(m_spare2);
        out.writeByte(m_spare3);

        m_topic.writeKryo(kryo, out);
    }

    public void readKryo(Kryo kryo, Input in) {
        m_headerVersion = in.readByte();
        m_version = in.readByte();
        m_priority = in.readByte();
        m_sequenceId = in.readLong(true);
        m_guid = in.readLong(true);
        int len = in.readByte();
        m_msgOrigAddr = new byte[len];
        in.read(m_msgOrigAddr, 0, len);

        m_spare1 = in.readByte();
        m_spare2 = in.readByte();
        m_spare3 = in.readByte();
        m_topic = new JetstreamTopic();
        m_topic.readKryo(kryo, in);
    
     }

}