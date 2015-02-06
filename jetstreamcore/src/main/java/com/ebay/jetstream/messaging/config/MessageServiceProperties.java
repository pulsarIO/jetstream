/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.NICUsage;
import com.ebay.jetstream.config.NotFoundException;
import com.ebay.jetstream.config.dns.DNSMap;
import com.ebay.jetstream.config.dns.DNSServiceRecord;
import com.ebay.jetstream.util.CommonUtils;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy (shmurthy@ebay.com)
 * 
 * MessageServiceProperties - Configuration for Message Service
 */

public class MessageServiceProperties extends AbstractNamedBean implements XSerializable {
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private NICUsage m_nicUsage;
	private DNSMap m_dnsMap;
	private String m_transport;
	private final Map<String, String> m_contextMap = new HashMap<String, String>();
	private ArrayList<TransportConfig> m_transports = new ArrayList<TransportConfig>();
	private final ArrayList<ContextConfig> m_mcastcontexts = new ArrayList<ContextConfig>();
	private final ArrayList<ContextConfig> m_ucastcontexts = new ArrayList<ContextConfig>();
	private int m_upstreamDispatchQueueSize = 30000;
	private int m_upstreamDispatchThreadPoolSize = 1;

	/**
	 * 
	 */
	public MessageServiceProperties() {
	}

		
	/**
	 * @return
	 */
	public Map<String, String> getContextMap() {
		return m_contextMap;
	}

	/**
	 * @return
	 */
	public DNSMap getDnsMap() {
		return m_dnsMap;
	}

	/**
	 * @return the mcastcontexts
	 */
	public ArrayList<ContextConfig> getMcastcontexts() {
		return m_mcastcontexts;
	}

	
	/**
	 * @return
	 */
	public NICUsage getNicUsage() {
		return m_nicUsage;
	}

	/**
	 * @return
	 */
	public String getTransport() {
		return m_transport;
	}

	/**
	 * @param transportName
	 * @return
	 */
	public TransportConfig getTransport(String transportName) {
		TransportConfig tport = null;

		Iterator<TransportConfig> itr = m_transports.iterator();

		while (itr.hasNext()) {
			tport = itr.next();

			if (tport.getTransportName().equals(transportName)) {
				return tport;
			}
		}

		return null;
	}

	/**
	 * @return the stacks
	 */
	public ArrayList<TransportConfig> getTransports() {
		return m_transports;
	}

	/**
	 * @return the ucastcontexts
	 */
	public ArrayList<ContextConfig> getUcastcontexts() {
		return m_ucastcontexts;
	}

	
	/**
	 * @return the upstreamDispatchQueueSize
	 */
	public int getUpstreamDispatchQueueSize() {
		return m_upstreamDispatchQueueSize;
	}

	/**
	 * @return the upstreamDispatchThreadPoolSize
	 */
	public int getUpstreamDispatchThreadPoolSize() {
		return m_upstreamDispatchThreadPoolSize;
	}

		
	/**
	 * @param transports
	 * @return
	 * @throws Exception 
	 */
	private boolean hasContextOverlapOverTransports(ArrayList<TransportConfig> transports) throws Exception {

		Iterator<TransportConfig> itr = m_transports.iterator();

		Map<String, Boolean> contextMap = new HashMap<String, Boolean>();

		while (itr.hasNext()) {

			TransportConfig tke = itr.next();

			Iterator<ContextConfig> ctxitr = tke.getContextList().iterator();

			while (ctxitr.hasNext()) {
						
				ContextConfig context = ctxitr.next();

				if (contextMap.containsKey(context.getContextname())) {
					LOGGER.error( "Context Overlap between transport instances - config needs to be changed");
					throw new Exception("Context Overlap between transport instances - context = " + context.getContextname());
				}
				else
					contextMap.put(context.getContextname(), new Boolean(true));

			}

		}

		return false;
	}

	
	
	/**
	 * @param dnsMap
	 */
	public void setDnsMap(DNSMap dnsMap) {
		m_dnsMap = dnsMap;
	}

	/**
	 * @param nicUsage
	 */
	public void setNicUsage(NICUsage nicUsage) {
		m_nicUsage = nicUsage;
	}

	/**
	 * @param transport
	 */
	public void setTransport(String transport) {
		m_transport = transport;
	}

	/**
	 * @param stacks
	 *          the stacks to set
	 * @throws Exception 
	 */
	public void setTransports(ArrayList<TransportConfig> transports) throws Exception {
		m_transports = transports;
		
		try {
			hasContextOverlapOverTransports(transports); // this method throws exception if there is a context overlap between transports

		} catch (Exception e) {
			throw e;
		}
		
	}

	/**
	 * @param upstreamDispatchQueueSize
	 *          the upstreamDispatchQueueSize to set
	 */
	public void setUpstreamDispatchQueueSize(int upstreamDispatchQueueSize) {
		m_upstreamDispatchQueueSize = upstreamDispatchQueueSize;
	}

	/**
	 * @param upstreamDispatchThreadPoolSize
	 *          the upstreamDispatchThreadPoolSize to set
	 */
	public void setUpstreamDispatchThreadPoolSize(int upstreamDispatchThreadPoolSize) {
		m_upstreamDispatchThreadPoolSize = upstreamDispatchThreadPoolSize;
	}

}
