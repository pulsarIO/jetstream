/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
* An implementation of a queue of request messages.
*
* *
* @author shmurthy@ebay.com
* @version 1.0
*/ 

public class InetAddressParser {

	private static class AddressElement {
		public int m_len =0;
		public char m_delimiter =0;
	}
	
	private String m_addrStr = null;
	private final byte [] m_ipaddr = new byte[4];
	private boolean m_hostAddrOnly = false;
	private boolean m_networkAddrOnly = false;
	private int m_prefixLen = 0;
	
	public InetAddressParser(String ipaddress)
	{
		m_addrStr = ipaddress;
	
		for (int i=0; i <4; i++)
		{
			m_ipaddr[i] = 0;
		}
		
		parse();
		
		
		
	}
	
	public boolean parse()
		{
		
		
		byte [] ipaddr = m_addrStr.getBytes();
		
		int addrLen = ipaddr.length;
	
		
		if (addrLen > 18)  return false;
		
		
		int offset = 0;
		int k = 0;
		char prevDelim = 0x1;
			
		AddressElement ae = new AddressElement();
		
		while (offset < addrLen)
		{
		
			String subStr = getAddressElement(ipaddr, offset, ae);
			
			
		      byte b = (byte) 0;
			  try {
			        b = Integer.decode(subStr).byteValue();
			   }
			   catch (NumberFormatException nfe) {
			      m_hostAddrOnly = false;
			       return false;
			      }

			
			if ((ae.m_delimiter == '.') || (ae.m_delimiter == '/'))
			{
				m_ipaddr[k++] = b;
				
			}
			else if ((ae.m_delimiter == 0) && (prevDelim == '/'))
			{
				m_prefixLen = b;
				if ((m_prefixLen > 32) || (m_prefixLen < 0))
					return false;
				else if (m_prefixLen == 32)
					m_hostAddrOnly = true;
				else if (m_prefixLen < 32)
					m_networkAddrOnly = true;
			}
			else if ((ae.m_delimiter == 0) && (prevDelim == '.'))
			{
				m_ipaddr[k] = b;
				
				m_prefixLen = 8 * (k+1);
				
				if (k < 3)
					m_networkAddrOnly = true;
				else
					m_hostAddrOnly = true;	
			}
			else if (ae.m_delimiter == 0)
			{
				m_ipaddr[k] = b;
				
						
				m_prefixLen = 8 * (k+1);
				
				if (k < 3)
					m_networkAddrOnly = true;
				else
					m_hostAddrOnly = true;	
			}
			
			prevDelim = ae.m_delimiter;
						
			offset += ae.m_len;
			
			offset += 1;
			
			ae.m_len=0;
			ae.m_delimiter=0;
			
		}
		return true;
	}
	
	public String getAddressElement(byte [] ipaddr, int offset, AddressElement ae)
	{
		getSubAddrLen(ipaddr, offset, ae);
		
		byte [] subaddr = new byte[ae.m_len];
		
		for (int j=0; j < ae.m_len; j++)
		{
			subaddr[j] = ipaddr[offset + j];
		}
		
		String subStr = new String(subaddr);
		
		return subStr;
	}
	
	
	private void getSubAddrLen(byte [] addr, int offset, AddressElement sa)
	{
	
		for (int i = offset; i < addr.length; i++)
		{
			if ((addr[i] == '.') || (addr[i] == '/'))
			{
				sa.m_delimiter = (char) addr[i];
				break;
			}
				
			
			sa.m_len += 1;
			
		}
				
	}
	
	public int getPrefixLen()
	{
		return m_prefixLen;
	}
	
	public boolean isHostAddress()
	{
		return m_hostAddrOnly;
	}
	
	public boolean isNetworkAddress()
	{
		return m_networkAddrOnly;
	}
	
	public byte [] getHostAddress()
	{
		return m_ipaddr;
	}
	
		

	public boolean matches(String provIpAddress, String provHostName)
	{
		InetAddressParser iap = new InetAddressParser(provIpAddress);
		
		if (!iap.isHostAddress()) return false;
		
		byte [] myaddr = getHostAddress();
		byte [] ipaddr = iap.getHostAddress();
		
		if (m_hostAddrOnly)
		{
			for(int i=0; i < 4; i++)
			{
				if(myaddr[i] != ipaddr[i])
					return false;
			}
		}
		
		try {
			
			
			String hostName = InetAddress.getByName(m_addrStr).getHostName();
			
			if (hostName.equals(provHostName))
				return true;
			
			return false;
		} catch (UnknownHostException e) {
			return false;
		}
		
		
	}
	
	
	/* address must be specified as xxx.yyy.zzz.kkk
	 * 
	 */
	
	public boolean matches(String address)
	{
		
		InetAddressParser iap = new InetAddressParser(address);
		
		byte [] myaddr = getHostAddress();
		byte [] ipaddr = iap.getHostAddress();
		
			
		if (iap.m_hostAddrOnly)
		{
			for(int i=0; i < 4; i++)
			{
								
				if(myaddr[i] != ipaddr[i])
					return false;
			}
		}
		else if (iap.m_networkAddrOnly)
		{
			
			
			int matchlen = 32 - iap.m_prefixLen;

			
			try {
				
				int addr1 = addressToInt(m_ipaddr) >> matchlen;
				int addr2 = addressToInt(iap.getHostAddress()) >> matchlen; 
		
				
				if (addr1 == addr2)
					return true;
				return false;
			} catch (Exception e) {
			}
			
		}
		return true;
		
	}
	
	public static final byte[] intToAddressBytes(int nCompactAddress) {
		return new byte[] {
				(byte)((nCompactAddress >> 24) & 0xff),
				(byte)((nCompactAddress >> 16) & 0xff),
				(byte)((nCompactAddress >> 8) & 0xff),
				(byte)(nCompactAddress & 0xff)
		};
	}
	
	public static final String intToAddressString(int nCompactAddress) {
		StringBuilder bldr = new StringBuilder(15);
		byte[] aBytes = intToAddressBytes(nCompactAddress);
		for (int i = 0; i < aBytes.length; i++) {
			if (i > 0)
				bldr.append('.');
			bldr.append(Byte.toString(aBytes[1]));
		}
		return bldr.toString();
	}
	
	public static final InetAddress intToAddress(int nCompactAddress) {
		try {
			return InetAddress.getByAddress(intToAddressBytes(nCompactAddress));
		} 
		catch (UnknownHostException e) {
			throw new IllegalArgumentException("Host not found: " + nCompactAddress, e);
		}
	}
	
	public static final int addressToInt(byte[] addr) {
		
		if (addr == null)
			throw new NullPointerException("input argument is null");
		
		if (addr.length != 4) 
			throw new IllegalArgumentException("address may not be null and must be IPv4 address of 4-byte length: " + new String(addr));
		
		return ((addr[0] & 0xff) << 24) | ((addr[1] & 0xff) << 16) | ((addr[2] & 0xff) << 8) | (addr[3] & 0xff); 
	}
	
	public static final int addressToInt(InetAddress addr) {
		
		if (addr == null) 
			throw new IllegalArgumentException("address may not be null");
		
		return InetAddressParser.addressToInt(addr.getAddress()); 
	}
	
	public static final int addressToInt(String addr) {
		
		if (addr == null) 
			throw new IllegalArgumentException("address may not be null");
		
		try {
			return InetAddressParser.addressToInt(InetAddress.getByName(addr));
		} 
		catch (UnknownHostException e) {
			throw new IllegalArgumentException("Host not found: " + addr, e);
		} 
	}
}
