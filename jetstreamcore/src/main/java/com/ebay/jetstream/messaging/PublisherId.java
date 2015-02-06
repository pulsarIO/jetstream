/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * @author shmurthy
 *
 * 
 */

public class PublisherId {

	String m_topicname;
	long m_guid =0;
	byte [] m_addr;
	
	/**
	 * 
	 */
	public PublisherId(){}
	
	/**
	 * @param topicname
	 * @param guid
	 * @param addr
	 */
	public PublisherId(String topicname, long guid, byte [] addr)
	{
		m_topicname = topicname;
		m_guid = guid;
		m_addr = new byte[addr.length];
		System.arraycopy(addr, 0, m_addr, 0, addr.length);

	}
	
	

	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		
		int hashCode = Arrays.hashCode(m_addr);
		hashCode += (Long.valueOf(m_guid)).hashCode();

		return hashCode;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		
		if (this == obj)
			return true;
			
		if (obj == null)
			return false;
		
		if (!(obj instanceof PublisherId))
			return false;

		PublisherId key = (PublisherId) obj;
		
		if(!Arrays.equals(m_addr, key.m_addr))
			return false;
		
		if (m_guid != key.m_guid)
			return false;

		return true;
	}
	
	/**
	 * @param guid
	 */
	public void setGuid(long guid)
	{
		m_guid = guid;
		
	}
	
	/**
	 * @param addr
	 */
	public void setAddr(byte [] addr)
	{
		m_addr = new byte[addr.length];
		System.arraycopy(addr, 0, m_addr, 0, addr.length);

	}
	
	/**
	 * @param topicname
	 */
	public void setTopicName(String topicname)
	{
		m_topicname = topicname;
		
	}
	
	public String getTopicname() {
		return m_topicname;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		
		String pubIdStr = "Id = " + Long.valueOf(m_guid).toString();
		
		pubIdStr += " ";
		
		try {
			pubIdStr += "Addr = " + InetAddress.getByAddress(m_addr).toString();
		} catch (UnknownHostException e) {
					
			pubIdStr += "Addr = Unknown";
		}
		
		return pubIdStr;
	}
	
	
	
}
