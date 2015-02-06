/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.interfaces;

import com.ebay.jetstream.config.NICUsage;
import com.ebay.jetstream.config.dns.DNSMap;
import com.ebay.jetstream.messaging.DispatchQueueStats;
import com.ebay.jetstream.messaging.MessageServiceProxy;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.MessageServiceProperties;
import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.stats.TransportStats;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;


/**
 * 
 * 
 * @author shmurthy (shmurthy@ebay.com)
 * 
 * Interface that must be implemented by all transport adaptor implementations.
 * MessageService allows transports to be implemented and plugged in to the message
 * service. There is a very lose coupling between the MessageService layer and the
 * transport adaptors. 
 */

public interface ITransportProvider {

	/**
	 * @return
	 */
	public TransportStats getStats();

	/**
	 * timed signal to gather stats
	 */
	public void harvestStats();

	/**
	 * /**
	 * 
	 * @throws Exception
	 */
	public void init(TransportConfig transportConfig, NICUsage nicUsage,
			DNSMap dnsMap, MessageServiceProxy proxy) throws Exception;

	/**
	 * @param topic
	 */
	public void pause(JetstreamTopic topic);
	
	/**
	 * @param topic
	 */
	public void resume(JetstreamTopic topic);
	
	/**
	 * @param topic
	 */
	public void unregisterTopic(JetstreamTopic topic);
	
	/**
	 * @param topic
	 */
	public void registerTopic(JetstreamTopic topic);
	
	/**
	 * @param context
	 */
	public void setContextConfig(ContextConfig cc) throws Exception;
	
	/**
	 * @return ContextConfig
	 */
	public ContextConfig getContextConfig();

	

	/**
	 * @param tl
	 * @throws Exception
	 */
	public void registerListener(ITransportListener tl) throws Exception;

	
	/**
	 * 
	 */
	public void resetStats();

	

	/**
	 * @param msg
	 * @throws Exception
	 */
	public void send(JetstreamMessage msg) throws Exception;

	/**
	 * @param addr
	 */
	public void setAddr(String addr);

	

	/**
	 * @param props
	 */
	public void setMessageServiceProperties(MessageServiceProperties props);

	/**
	 * @param port
	 */
	public void setPort(int port);

	/**
	 * @param stats
	 */
	public void setUpstreamDispatchQueueStats(DispatchQueueStats stats);

	/**
	 * @throws MessageServiceException
	 */
	public void shutdown() throws MessageServiceException;

	
	/**
	 * @param context
	 */
	public void prepareToPublish(String context);
	
	
	/**
	 * @return
	 */
	public TransportConfig getTransportConfig();

}
