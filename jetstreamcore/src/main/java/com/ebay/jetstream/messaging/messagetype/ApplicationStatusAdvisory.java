/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.messaging.messagetype;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


/**
 * 
 * This Advisory Message is used for Messaging Applications to signal advisory messages notifying App Specific
 * Failures to a remote message listener.
 * 
 * @author varavindan
 * 
 */
public class ApplicationStatusAdvisory extends JetstreamMessage implements Externalizable {

  /**
   * Holds the Failure Conditions which will be notified by the App
   * 
   * @author varavindan
   * 
   */
  public enum FaultEnum {
    DB_ACCESS_FAILURE, GENERIC_FAILURE
  }

  private String m_eventType = null;

  /**
   * Holds the message origin source/app
   */
  private String m_msgEventSource;

  /**
   * Holds the splitNumber which was either got marked up or marked down
   */
  private List<Integer> m_splitNumber;

  /**
   * Holds the core failure cause observed in the app
   */
  private FaultEnum m_faultDetail;

  /**
   * Indicates whether a reported failure got resolved
   */
  private boolean m_isResolved;

  private long m_advisoryWriteTime;

  /**
   * Holds meaningful failure description for logging purposes
   */
  private String m_faultString;

  public static final String FAULT_REASON = "Reason";

  public static final String SPILIT_NUMBER = "Split_No";

  public static final String RESOLVED_STATUS = "Resolved_Status";

  public static final String EVENT_TYPE = "EventType";

  public static final String EVENT_SOURCE = "EventSource";

  private String forwardingTopic;

  /**
   * 
   */
  public ApplicationStatusAdvisory() {

  }

  /**
   * @param eflEventSource
   * @param splitNumber
   * @param faultDetail
   * @param isResolved
   * @param faultString
   */
  public ApplicationStatusAdvisory(String eflEventSource, List<Integer> splitNumber, FaultEnum faultDetail,
      boolean isResolved, String faultString, String eventType) throws Exception {

    super();
    m_msgEventSource = eflEventSource;
    m_splitNumber = splitNumber;
    m_faultDetail = faultDetail;
    m_isResolved = isResolved;
    m_faultString = faultString;
    m_eventType = eventType;
  }

  /**
   * @return the advisoryWriteTime
   */
  public long getAdvisoryWriteTime() {
    return m_advisoryWriteTime;
  }

  /**
   * @return the eflEventSource
   */
  public String getEflEventSource() {
    return m_msgEventSource;
  }

  /**
   * @return the eventType
   */
  public String getEventType() {
    return m_eventType;
  }

  /**
   * @return the faultDetail
   */
  public FaultEnum getFaultDetail() {
    return m_faultDetail;
  }

  /**
   * @return the faultString
   */
  public String getFaultString() {
    return m_faultString;
  }

  public String getForwardingTopic() {
    return forwardingTopic;
  }

  /**
   * @return the msgEventSource
   */
  public String getMsgEventSource() {
    return m_msgEventSource;
  }

  /**
   * @return the splitNumber
   */
  public List<Integer> getSplitNumber() {
    return m_splitNumber;
  }

  /**
   * @return the isResolved
   */
  public boolean isResolved() {
    return m_isResolved;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException {
    try {
      m_splitNumber = (List<Integer>) in.readObject();
      m_eventType = (String) in.readObject();
      m_faultDetail = (FaultEnum) in.readObject();
      m_faultString = (String) in.readObject();
      m_isResolved = in.readBoolean();
      m_msgEventSource = (String) in.readObject();
      forwardingTopic = (String) in.readObject();
      m_advisoryWriteTime = in.readLong();
    }
    catch (ClassNotFoundException c) {
      
    }
    super.readExternal(in);
  }

  /**
   * @param advisoryWriteTime
   *          the advisoryWriteTime to set
   */
  public void setAdvisoryWriteTime(long advisoryWriteTime) {
    m_advisoryWriteTime = advisoryWriteTime;
  }

  /**
   * @param eventType
   *          the eventType to set
   */
  public void setEventType(String eventType) {
    m_eventType = eventType;
  }

  /**
   * @param faultDetail
   *          the faultDetail to set
   */
  public void setFaultDetail(FaultEnum faultDetail) {
    m_faultDetail = faultDetail;
  }

  /**
   * @param faultString
   *          the faultString to set
   */
  public void setFaultString(String faultString) {
    m_faultString = faultString;
  }

  public void setForwardingTopic(String forwardingTopic) {
    this.forwardingTopic = forwardingTopic;
  }

  /**
   * @param msgEventSource
   *          the msgEventSource to set
   */
  public void setMsgEventSource(String msgEventSource) {
    m_msgEventSource = msgEventSource;
  }

  /**
   * @param isResolved
   *          the isResolved to set
   */
  public void setResolved(boolean isResolved) {
    m_isResolved = isResolved;
  }

  /**
   * @param splitNumber
   *          the splitNumber to set
   */
  public void setSplitNumber(List<Integer> splitNumber) {
    m_splitNumber = splitNumber;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ebay.jetstream.messaging.JetstreamMessage#toString()
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.getClass().getSimpleName()).append(": ").append(EVENT_SOURCE).append("=").append(m_msgEventSource)
        .append(",").append(EVENT_TYPE).append("=").append(m_eventType).append(",").append(SPILIT_NUMBER).append("=")
        .append(m_splitNumber).append(",").append(RESOLVED_STATUS).append("=").append(m_isResolved).append(",")
        .append(FAULT_REASON).append("=").append(m_faultDetail).append(":").append(m_faultString).append(", Topics=")
        .append(getTopic()).append(", ForwardingTopic=").append(forwardingTopic);
    return sb.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(m_splitNumber);
    out.writeObject(m_eventType);
    out.writeObject(m_faultDetail);
    out.writeObject(m_faultString);
    out.writeBoolean(m_isResolved);
    out.writeObject(m_msgEventSource);
    out.writeObject(forwardingTopic);
    out.writeLong(m_advisoryWriteTime);

    super.writeExternal(out);
  }

}
