/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.config;

import java.util.ArrayList;
import java.util.Iterator;

import com.ebay.jetstream.xmlser.XSerializable;


/**
 * @author shmurthy (shmurthy@ebay.com)
 * 
 *         Transport specific config - Base class for all transport configs
 */
public class TransportConfig implements XSerializable {
	private String m_transportClass;
	private ArrayList<ContextConfig> m_contextList;
	private int m_sendbuffersize = 0;
	private int m_receivebuffersize = 0;
	private int m_downstreamDispatchQueueSize = 200000;
	private String m_transportName = "";
	private int m_consumerThreadPoolSize = 1;
	private String m_protocol;
	private boolean m_requireDNS = false;
	private String m_netmask = "127.0.0.1/8";

	
	public String getNetmask() {
		return m_netmask;
	}

	public void setNetmask(String netmask) {
		this.m_netmask = netmask;
	}

	public boolean requireDNS() {
		return m_requireDNS;
	}

	public void setRequireDNS(boolean m_requireDNS) {
		this.m_requireDNS = m_requireDNS;
	}

	/**
	 * @return the consumerThreadPoolSize
	 */
	public int getConsumerThreadPoolSize() {
		return m_consumerThreadPoolSize;
	}

	/**
	 * @return
	 */
	public ArrayList<ContextConfig> getContextList() {
		return m_contextList;
	}

	/**
	 * @return the downstreamDispatchQueueSize
	 */
	public int getDownstreamDispatchQueueSize() {
		return m_downstreamDispatchQueueSize;
	}

	/**
	 * @return the protocol
	 */
	public String getProtocol() {
		return m_protocol;
	}

	/**
	 * @return the receivebuffersize
	 */
	public int getReceivebuffersize() {
		return m_receivebuffersize;
	}

	/**
	 * @return the sendbuffersize
	 */
	public int getSendbuffersize() {
		return m_sendbuffersize;
	}

	/**
	 * @return
	 */
	public String getTransportClass() {
		return m_transportClass;
	}

	/**
	 * @return the transportName
	 */
	public String getTransportName() {
		return m_transportName;
	}

	
	/**
	 * @param consumerThreadPoolSize
	 *            the consumerThreadPoolSize to set
	 */
	public void setConsumerThreadPoolSize(int consumerThreadPoolSize) {
		m_consumerThreadPoolSize = consumerThreadPoolSize;
	}

	/**
	 * @param contextList
	 */
	public void setContextList(ArrayList<ContextConfig> contextList) {
		m_contextList = contextList;
	}

	/**
	 * @param downstreamDispatchQueueSize
	 *            the downstreamDispatchQueueSize to set
	 */
	public void setDownstreamDispatchQueueSize(int downstreamDispatchQueueSize) {
		m_downstreamDispatchQueueSize = downstreamDispatchQueueSize;
	}

	/**
	 * @param protocol
	 *            the protocol to set
	 */
	public void setProtocol(String protocol) {
		m_protocol = protocol;
	}

	/**
	 * @param receivebuffersize
	 *            the receivebuffersize to set
	 */
	public void setReceivebuffersize(int receivebuffersize) {
		m_receivebuffersize = receivebuffersize;
	}

	/**
	 * @param sendbuffersize
	 *            the sendbuffersize to set
	 */
	public void setSendbuffersize(int sendbuffersize) {
		m_sendbuffersize = sendbuffersize;
	}

	/**
	 * @param className
	 */
	public void setTransportClass(String className) {
		m_transportClass = className;
	}

	/**
	 * @param transportName
	 *            the transportName to set
	 */
	public void setTransportName(String transportName) {
		m_transportName = transportName;
	}

	public ContextConfig getContextConfig(String contextName) {
		Iterator<ContextConfig> itr = m_contextList.iterator();

		ContextConfig cc = null;

		while (itr.hasNext()) {
			cc = itr.next();
			if (contextName.equals(cc.getContextname()))
				break;
		}

		return cc;
	}
	
	public boolean isContextBindingsChanged(TransportConfig other) {
		for(ContextConfig cc : m_contextList){
			boolean bMatch = false;
    		for(ContextConfig othercc : other.m_contextList){
    			if (cc.equals(othercc)) {
    				bMatch = true;
    				 break;
    			}
    		}
    		if ((!bMatch)) return true;
    	}
		
		return false;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof TransportConfig)) {
			return false;
		}
		TransportConfig other = (TransportConfig) obj;
		if (m_consumerThreadPoolSize != other.m_consumerThreadPoolSize) {
			return false;
		}
		if (m_downstreamDispatchQueueSize != other.m_downstreamDispatchQueueSize) {
			return false;
		}
		if (m_protocol == null) {
			if (other.m_protocol != null) {
				return false;
			}
		} else if (!m_protocol.equals(other.m_protocol)) {
			return false;
		}
		if (m_receivebuffersize != other.m_receivebuffersize) {
			return false;
		}
		if (m_sendbuffersize != other.m_sendbuffersize) {
			return false;
		}
		if (m_transportClass == null) {
			if (other.m_transportClass != null) {
				return false;
			}
		} else if (!m_transportClass.equals(other.m_transportClass)) {
			return false;
		}
		if (m_transportName == null) {
			if (other.m_transportName != null) {
				return false;
			}
		} else if (!m_transportName.equals(other.m_transportName)) {
			return false;
		}
				
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + m_consumerThreadPoolSize;
		result = prime * result + m_downstreamDispatchQueueSize;
		result = prime * result
				+ ((m_protocol == null) ? 0 : m_protocol.hashCode());
		result = prime * result + m_receivebuffersize;
		result = prime * result + m_sendbuffersize;
		result = prime
				* result
				+ ((m_transportClass == null) ? 0 : m_transportClass.hashCode());
		result = prime * result
				+ ((m_transportName == null) ? 0 : m_transportName.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return "TransportConfig [m_transportClass=" + m_transportClass
				+ "\n m_contextList=" + m_contextList + "\n m_sendbuffersize="
				+ m_sendbuffersize + "\n m_receivebuffersize="
				+ m_receivebuffersize + "\n m_downstreamDispatchQueueSize="
				+ m_downstreamDispatchQueueSize + "\n m_transportName="
				+ m_transportName + ", m_consumerThreadPoolSize="
				+ m_consumerThreadPoolSize + "\n m_protocol=" + m_protocol
				+ "\n m_requireDNS=" + m_requireDNS 
				+ "\n netmask=" + m_netmask + "]";
	}

}
