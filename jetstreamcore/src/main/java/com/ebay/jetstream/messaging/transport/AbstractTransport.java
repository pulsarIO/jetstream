/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 *
 *          All transport impls must extend this
 * 
 */

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.NICUsage;
import com.ebay.jetstream.messaging.config.MessageServiceProperties;
import com.ebay.jetstream.messaging.interfaces.ITransportProvider;

public abstract class AbstractTransport implements ITransportProvider {
	
	 private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	 private List<InetAddress> m_ifcAddrList = new LinkedList<InetAddress>();;
	 protected MessageServiceProperties m_props;
	 
	
	 public void setMessageServiceProperties(MessageServiceProperties props) {
		    m_props = props;
	 }
	 
	 public MessageServiceProperties getMessageServiceProperties() {
		    return m_props;
	 }
	 protected void addLoopBackIfc(List<InetAddress> ifcAddrList) {

		    Enumeration<NetworkInterface> e;

		    try {
		      e = NetworkInterface.getNetworkInterfaces();

		      while (e.hasMoreElements()) {
		        NetworkInterface newnic = e.nextElement();

		        Enumeration<InetAddress> addrs = newnic.getInetAddresses();

		        while (addrs.hasMoreElements()) {
		          InetAddress inetaddr = addrs.nextElement();

		          try {
		            if (inetaddr.getHostAddress().equals("127.0.0.1")) {
		              ifcAddrList.add(inetaddr);
		              return;
		            }

		          }
		          catch (Exception e1) {

		            String message = "Interface " + newnic.getDisplayName()
		                + " does not support multicast - Exception - ";

		            if (LOGGER.isWarnEnabled()) {

		              message += e1.getMessage();

		              LOGGER.warn( message);
		            }
		          }
		        } // end of while

		      }

		    }
		    catch (SocketException e1) {

		      if (LOGGER.isErrorEnabled()) {
		       
		        LOGGER.error( "Failed to retrieve Interfaces on this host -"  + e1.getMessage());
		      }

		    }

		  }

		  		  
		  protected List<InetAddress> findIfcOnHost(String serviceType) {
				List<InetAddress> ifcAddrList = null;
				NICUsage nic = m_props.getNicUsage();

				ifcAddrList = nic.getInetAddressListByUsage(serviceType);

				if (ifcAddrList == null)
					ifcAddrList = new LinkedList<InetAddress>();

				if (ifcAddrList.size() == 0)
					addLoopBackIfc(ifcAddrList);

				return ifcAddrList;
			}
		  

	
}
