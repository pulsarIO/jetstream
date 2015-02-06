/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.messaging.transport.netty.config;

import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.topic.TopicDefs;
import com.ebay.jetstream.notification.AlertListener;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy
 * Netty Transport Config
 * 
 */

public class NettyTransportConfig extends TransportConfig implements XSerializable {

	private int m_connectionTimeoutInSecs = 10; // netty specific
	private int m_idleTimeoutInSecs = 6*3600; // netty specific
	private int m_writeTimeoutInSecs = 10; // netty specific
	private boolean m_asyncConnect = true; // netty specific
	private long m_maxNettyBackLog = 20000;
	private int m_numConnectorIoProcessors = Math.min(1, Runtime.getRuntime()
			.availableProcessors()); // netty Specific
	private int m_numAcceptorIoProcessors = Math.min(1, Runtime.getRuntime()
			.availableProcessors()); // netty Specific
	private int m_connectionPoolSz = 1;
	private int m_autoFlushSz = 0;
	private int m_autoFlushTimeInterval = 500; // default is 500 millis
	private AlertListener m_alertListener;
	private long m_vqOverflowThreshold = 5;
	private long m_vqOFMeasurementInterval = 60000L;
	private String m_advertisementTopic = TopicDefs.JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG;
	private String m_discoverTopic = TopicDefs.JETSTREAM_EVENT_CONSUMER_DISCOVER_MSG;
	private boolean m_enableCompression = false;
	private boolean m_tcpKeepAlive = false;
	private boolean m_enableAcking = false;
	private long m_proactiveConnCloseIntervalMs = -1; // -1 to disable the connection close, default -1 
	

	public boolean isAckingEnabled() {
		return m_enableAcking;
	}
	
	public boolean getEnableAcking() {
		return m_enableAcking;
	}

	public void setEnableAcking(boolean enableAcking) {
		this.m_enableAcking = enableAcking;
	}

	public void setTcpKeepAlive(boolean tcpKeepAlive) {
		this.m_tcpKeepAlive = tcpKeepAlive;
	}
	
	public boolean getTcpKeepAlive() {
		return m_tcpKeepAlive;
	}

	public boolean isEnableCompression() {
		return m_enableCompression;
	}

	public boolean getEnableCompression() {
		return m_enableCompression;
	}
	public void setEnableCompression(boolean enableCompression) {
		this.m_enableCompression = enableCompression;
	}

	public String getDiscoverTopic() {
		return m_discoverTopic;
	}

	public void setDiscoverTopic(String discoverTopic) {
		this.m_discoverTopic = discoverTopic;
	}

	public String getAdvertisementTopic() {
		return m_advertisementTopic;
	}

	public void setAdvertisementTopic(String m_advertisementTopic) {
		this.m_advertisementTopic = m_advertisementTopic;
	}

	public long getVqOverflowThreshold() {
		return m_vqOverflowThreshold;
	}

	public void setVqOverflowThreshold(long vqOverflowThreshold) {
		this.m_vqOverflowThreshold = vqOverflowThreshold;
	}

	public long getVqOFMeasurementInterval() {
		return m_vqOFMeasurementInterval;
	}

	public void setVqOFMeasurementInterval(long vqOFMeasurementInterval) {
		this.m_vqOFMeasurementInterval = vqOFMeasurementInterval;
	}

	public AlertListener getAlertListener() {
		return m_alertListener;
	}

	public void setAlertListener(AlertListener alertListener) {
		this.m_alertListener = alertListener;
	}

	public void setAutoFlushTimeInterval(int autoFlushTimeInterval) {
		this.m_autoFlushTimeInterval = autoFlushTimeInterval;
	}

	public int getAutoFlushSz() {
		return m_autoFlushSz;
	}

	public void setAutoFlushSz(int autoFlushSz) {
		this.m_autoFlushSz = autoFlushSz;
	}

	public int getConnectionPoolSz() {
		return m_connectionPoolSz;
	}

	public void setConnectionPoolSz(int connectionPoolSz) {
		if(connectionPoolSz < 1)
			this.m_connectionPoolSz = 1;
		else
			this.m_connectionPoolSz = connectionPoolSz;
	}

		
	/**
	 * @return
	 */
	public long getMaxNettyBackLog() {
		return m_maxNettyBackLog;
	}

	/**
	 * @param maxNettyBackLog
	 */
	public void setMaxNettyBackLog(long maxNettyBackLog) {
		this.m_maxNettyBackLog = maxNettyBackLog;
	}

	

	/**
	 * @return the connectionTimeoutInSecs
	 */
	public int getConnectionTimeoutInSecs() {
		return m_connectionTimeoutInSecs;
	}

	
	/**
	 * @return the idleTimeoutInSecs
	 */
	public int getIdleTimeoutInSecs() {
		return m_idleTimeoutInSecs;
	}

	/**
	 * @return the numAcceptorIoProcessors
	 */
	public int getNumAcceptorIoProcessors() {
		return m_numAcceptorIoProcessors;
	}

	/**
	 * @return the numConnectorIoProcessors
	 */
	public int getNumConnectorIoProcessors() {
		return m_numConnectorIoProcessors;
	}

	
	/**
	 * @return the writeTimeoutInSecs
	 */
	public int getWriteTimeoutInSecs() {
		return m_writeTimeoutInSecs;
	}

	
	/**
	 * @return the asyncConnect
	 */
	public boolean isAsyncConnect() {
		return m_asyncConnect;
	}

	
	
	/**
	 * @param asyncConnect
	 *            the asyncConnect to set
	 */
	public void setAsyncConnect(boolean asyncConnect) {
		m_asyncConnect = asyncConnect;
	}

	/**
	 * @param connectionTimeoutInSecs
	 *            the connectionTimeoutInSecs to set
	 */
	public void setConnectionTimeoutInSecs(int connectionTimeoutInSecs) {
		m_connectionTimeoutInSecs = connectionTimeoutInSecs;
	}

	

	/**
	 * @param idleTimeoutInSecs
	 *            the idleTimeoutInSecs to set
	 */
	public void setIdleTimeoutInSecs(int idleTimeoutInSecs) {
		m_idleTimeoutInSecs = idleTimeoutInSecs;
	}

	/**
	 * @param numAcceptorIoProcessors
	 *            the numAcceptorIoProcessors to set
	 */
	public void setNumAcceptorIoProcessors(int numAcceptorIoProcessors) {
		m_numAcceptorIoProcessors = numAcceptorIoProcessors;
	}

	/**
	 * @param numConnectorIoProcessors
	 *            the numConnectorIoProcessors to set
	 */
	public void setNumConnectorIoProcessors(int numConnectorIoProcessors) {
		m_numConnectorIoProcessors = numConnectorIoProcessors;
	}

	
	
	/**
	 * @param writeTimeoutInSecs
	 *            the writeTimeoutInSecs to set
	 */
	public void setWriteTimeoutInSecs(int writeTimeoutInSecs) {
		m_writeTimeoutInSecs = writeTimeoutInSecs;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime
				* result
				+ ((m_advertisementTopic == null) ? 0 : m_advertisementTopic
						.hashCode());
		result = prime * result
				+ ((m_alertListener == null) ? 0 : m_alertListener.hashCode());
		result = prime * result + (m_asyncConnect ? 1231 : 1237);
		result = prime * result + m_autoFlushSz;
		result = prime * result + m_autoFlushTimeInterval;
		result = prime * result + m_connectionPoolSz;
		result = prime * result + m_connectionTimeoutInSecs;
		result = prime * result
				+ ((m_discoverTopic == null) ? 0 : m_discoverTopic.hashCode());
		result = prime * result + (m_enableAcking ? 1231 : 1237);
		result = prime * result + (m_enableCompression ? 1231 : 1237);
		result = prime * result + m_idleTimeoutInSecs;
		result = prime * result
				+ (int) (m_maxNettyBackLog ^ (m_maxNettyBackLog >>> 32));
		result = prime * result + m_numAcceptorIoProcessors;
		result = prime * result + m_numConnectorIoProcessors;
		result = prime
				* result
				+ (int) (m_proactiveConnCloseIntervalMs ^ (m_proactiveConnCloseIntervalMs >>> 32));
		result = prime * result + (m_tcpKeepAlive ? 1231 : 1237);
		result = prime
				* result
				+ (int) (m_vqOFMeasurementInterval ^ (m_vqOFMeasurementInterval >>> 32));
		result = prime
				* result
				+ (int) (m_vqOverflowThreshold ^ (m_vqOverflowThreshold >>> 32));
		result = prime * result + m_writeTimeoutInSecs;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		NettyTransportConfig other = (NettyTransportConfig) obj;
		if (m_advertisementTopic == null) {
			if (other.m_advertisementTopic != null)
				return false;
		} else if (!m_advertisementTopic.equals(other.m_advertisementTopic))
			return false;
		if (m_alertListener == null) {
			if (other.m_alertListener != null)
				return false;
		} else if (!m_alertListener.equals(other.m_alertListener))
			return false;
		if (m_asyncConnect != other.m_asyncConnect)
			return false;
		if (m_autoFlushSz != other.m_autoFlushSz)
			return false;
		if (m_autoFlushTimeInterval != other.m_autoFlushTimeInterval)
			return false;
		if (m_connectionPoolSz != other.m_connectionPoolSz)
			return false;
		if (m_connectionTimeoutInSecs != other.m_connectionTimeoutInSecs)
			return false;
		if (m_discoverTopic == null) {
			if (other.m_discoverTopic != null)
				return false;
		} else if (!m_discoverTopic.equals(other.m_discoverTopic))
			return false;
		if (m_enableAcking != other.m_enableAcking)
			return false;
		if (m_enableCompression != other.m_enableCompression)
			return false;
		if (m_idleTimeoutInSecs != other.m_idleTimeoutInSecs)
			return false;
		if (m_maxNettyBackLog != other.m_maxNettyBackLog)
			return false;
		if (m_numAcceptorIoProcessors != other.m_numAcceptorIoProcessors)
			return false;
		if (m_numConnectorIoProcessors != other.m_numConnectorIoProcessors)
			return false;
		if (m_proactiveConnCloseIntervalMs != other.m_proactiveConnCloseIntervalMs)
			return false;
		if (m_tcpKeepAlive != other.m_tcpKeepAlive)
			return false;
		if (m_vqOFMeasurementInterval != other.m_vqOFMeasurementInterval)
			return false;
		if (m_vqOverflowThreshold != other.m_vqOverflowThreshold)
			return false;
		if (m_writeTimeoutInSecs != other.m_writeTimeoutInSecs)
			return false;
		return true;
	}
	

	
	

	@Override
	public String toString() {
		return "NettyTransportConfig [m_connectionTimeoutInSecs="
				+ m_connectionTimeoutInSecs + ", m_idleTimeoutInSecs="
				+ m_idleTimeoutInSecs + ", m_writeTimeoutInSecs="
				+ m_writeTimeoutInSecs + ", m_asyncConnect=" + m_asyncConnect
				+ ", m_maxNettyBackLog=" + m_maxNettyBackLog
				+ ", m_numConnectorIoProcessors=" + m_numConnectorIoProcessors
				+ ", m_numAcceptorIoProcessors=" + m_numAcceptorIoProcessors
				+ ", m_connectionPoolSz=" + m_connectionPoolSz
				+ ", m_autoFlushSz=" + m_autoFlushSz
				+ ", m_autoFlushTimeInterval=" + m_autoFlushTimeInterval
				+ ", m_alertListener=" + m_alertListener
				+ ", m_vqOverflowThreshold=" + m_vqOverflowThreshold
				+ ", m_vqOFMeasurementInterval=" + m_vqOFMeasurementInterval
				+ ", m_advertisementTopic=" + m_advertisementTopic
				+ ", m_discoverTopic=" + m_discoverTopic
				+ ", m_enableCompression=" + m_enableCompression
				+ ", m_tcpKeepAlive=" + m_tcpKeepAlive + ", m_enableAcking="
				+ m_enableAcking + ", " +				
				", m_overflowBufferSzInMB="
				+ ", m_proactiveConnCloseIntervalMs="
				+ m_proactiveConnCloseIntervalMs + "]";
	}

	public boolean installAutoFlushHandler() {
		
		if (m_autoFlushSz == 0)
			return false;
		else
			return true;
		
	}

	public int getAutoFlushTimeInterval() {
		return m_autoFlushTimeInterval;
	}

	public long getProactiveConnCloseIntervalMs() {
		return m_proactiveConnCloseIntervalMs;
	}

	public void setProactiveConnCloseIntervalMs(
			long proactiveConnCloseIntervalMs) {
		m_proactiveConnCloseIntervalMs = proactiveConnCloseIntervalMs;
	}
	
	
}
