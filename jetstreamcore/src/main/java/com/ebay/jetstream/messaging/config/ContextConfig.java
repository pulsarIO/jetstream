/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.config;


import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy (shmurthy@ebay.com)
 * 
 *  Context Specific configuration
 */

@ManagedResource
public class ContextConfig implements XSerializable {

	private String m_contextname;
	private int m_port = -1;
	private String m_ipaddress = "0.0.0.0";
	
	

	public String getIpaddress() {
		return m_ipaddress;
	}

	public void setIpaddress(String ipaddress) {
		this.m_ipaddress = ipaddress;
	}

	public int getPort() {
		return m_port;
	}

	public void setPort(int port) {
		this.m_port = port;
	}

	
	public String getContextname() {
		return m_contextname;
	}

	public void setContextname(String contextname) {
		this.m_contextname = contextname;
	}
	
	public String getHostAndPort() {
		StringBuffer buf = new StringBuffer();
		
		buf.append(m_ipaddress);
		buf.append(",");
		buf.append(m_port);
		
		return buf.toString();
			
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

		if (!(obj instanceof ContextConfig))
			return false;

		ContextConfig cc = (ContextConfig) obj;

		if (!m_contextname.equals(cc.getContextname()))
			return false;

		return true;
	}
	
	public int hashCode() {
				
		return m_contextname.hashCode() + m_ipaddress.hashCode();
		
	}

}
