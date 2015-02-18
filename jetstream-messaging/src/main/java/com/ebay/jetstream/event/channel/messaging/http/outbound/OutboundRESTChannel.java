/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging.http.outbound;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.channel.AbstractOutboundChannel;
import com.ebay.jetstream.event.channel.ChannelAddress;
import com.ebay.jetstream.event.channel.ChannelAlarmListener;
import com.ebay.jetstream.event.support.ErrorManager;
import com.ebay.jetstream.http.netty.client.HttpClient;
import com.ebay.jetstream.http.netty.client.HttpClientConfig;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.Hidden;

@ManagedResource(objectName = "Event/Channel", description = "Outbound REST Channe")
public class OutboundRESTChannel extends AbstractOutboundChannel implements ChannelFutureListener {
	
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.http.outbound");
	private RESTChannelAddress m_address;
	private ChannelAlarmListener m_channelAlarmListener;
	private final LongCounter m_retryAdvisoryEvents = new LongCounter();
	private final ConcurrentHashMap<String, AtomicBoolean> m_alarmState = new ConcurrentHashMap<String, AtomicBoolean>();
	private Advice m_adviceListener = null;
	private final LongCounter m_eventSentToAdviceListener = new LongCounter();
	private boolean m_constructEventHolder = false;
	private HttpClientConfig m_config = new HttpClientConfig();
	private HttpClient m_client;
	private EventBatcher m_batchPublisher;
	private CopyOnWriteArrayList<URI> m_urlList = new CopyOnWriteArrayList<URI>();
	private ErrorManager m_errors = new ErrorManager();
	
	public ErrorManager getErrors() {
		return m_errors;
	}
	
	public EventBatcher getBatchPublisher() {
		return m_batchPublisher;
	}

	public void setBatchPublisher(EventBatcher m_batchPublisher) {
		this.m_batchPublisher = m_batchPublisher;
	}

	private Thread m_queueProcessor;

	public OutboundRESTChannel() {
	}

	public void afterPropertiesSet() throws Exception {
				
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#close()
	 */
	public void close() throws EventException {
		LOGGER.info( "Closing Outbound Messaging Channel");
	
		super.close();
		
		Management.removeBeanOrFolder(getBeanName(), this);
		m_client.shutdown();
		m_batchPublisher.shutdown();
		m_urlList.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#flush()
	 */
	public void flush() throws EventException {
		// This is a NOOP for this channel
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#getAddress()
	 */
	public ChannelAddress getAddress() {
		return m_address;
	}

	/**
	 * @return the advisoryListener
	 */
	public Advice getAdviceListener() {
		return m_adviceListener;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.OutboundChannel#getAlarmListener()
	 */
	@Hidden
	public ChannelAlarmListener getAlarmListener() {
		return m_channelAlarmListener;
	}

		
	/**
	 * @return the config
	 */
	public HttpClientConfig getConfig() {
		return m_config;
	}

	public int getPendingEvents() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public long getEventSentToAdviceListener() {
		return m_eventSentToAdviceListener.get();
	}

		
	public long getRetryAdvisoryEvents() {
		return m_retryAdvisoryEvents.get();
	}

	public boolean isConstructEventHolder() {
		return m_constructEventHolder;
	}

	public void onApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {
			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;
			// Calculate changes
			if (bcInfo.isChangedBean(getConfig())) {
				close();
				setConfig((HttpClientConfig) bcInfo.getChangedBean());
				open();
			}
			else if (bcInfo.isChangedBean(getAddress())) {
				setAddress((ChannelAddress) bcInfo.getChangedBean());
				m_urlList.clear();
				m_urlList.addAll(m_address.getUriList());
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#open()
	 */
	public void open() throws EventException {
		LOGGER.info( "Opening Outbound Messaging Channel");

		super.open();
		
		Management.removeBeanOrFolder(getBeanName(), this);
		Management.addBean(getBeanName(), this);
		m_client = new HttpClient();
		m_client.setConfig(getConfig());
		m_client.setKeepAlive(true);
		m_batchPublisher = new EventBatcher(m_client);
		m_batchPublisher.setBatchSize(m_config.getBatchSz());
		m_queueProcessor = new Thread(m_batchPublisher);
		m_queueProcessor.start();
		m_urlList.addAll(m_address.getUriList());
		
		try {
			Iterator<URI> urlIter = m_urlList.iterator();

			while(urlIter.hasNext()) {
				m_client.connect(urlIter.next(), getConfig().getNumConnections());
			}
		}
		catch (UnknownHostException e) {
			m_errors.registerError(e);
			throw new EventException(e.getLocalizedMessage());
		}
	}

	@Override
	public void operationComplete(ChannelFuture future) throws Exception {
		// TODO Auto-generated method stub

	}


	
	
	
	/**
	 * 
	 * Resets the rootcontext in hashmap to false indicating alarm has been cleared on the topic
	 * 
	 * @param rootContext
	 */
	private void resetTopicAlarmState(String rootContext) {
		m_alarmState.get(rootContext).set(false);
	}
	
	@Override
	public long getTotalEventsDropped() {
		incrementEventDroppedCounter(m_client.getTotalEventsDropped(true));
		return super.getTotalEventsDropped();
	}
	
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.EventSink#sendEvent(com.ebay.jetstream.event.JetstreamEvent)
	 */
	public void sendEvent(JetstreamEvent event) throws EventException {

		// TBD : move to a queue mode to send at a later time we need to correlate request with response

        incrementEventRecievedCounter();
        
		if (m_client != null) {

			try {

				List<URI> uriList = m_address.getUriList();

				String[] forwardinguris = getForwardingURIList(event);
				
				for (URI uri : uriList) {
					
					if (forwardinguris != null) {
						boolean bFound = false;
						String fwduri = uri.toString();
						for (String provuri : forwardinguris) {
							if (provuri.equals(fwduri)) {
								bFound = true;
								break;
							}
						}
						if (!bFound)
							continue;
					}
					
					m_batchPublisher.submit(uri, event);
					setLastEvent(event);
				}
	
								
				
			}
			catch (Throwable t) {
				m_errors.registerError(t);
				incrementEventDroppedCounter();
				LOGGER.warn( "Failed to submit event " + t.getLocalizedMessage());
				
			}
		}

	}
	
	
	private String[] getForwardingURIList(JetstreamEvent event) {

		String[] forwardingUris= event.getForwardingUrls();
	
		if (forwardingUris != null) {
			
			for (String str : forwardingUris) {
				if (!m_address.getUrlList().contains(str)) {
					
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Either Topic " + str
								+ " is undefined in configuration (check message service setup & dns setup) or the EPL is incorrect");
					}
				}
			}
		}

		return forwardingUris;

	}
		

	private void sendToAdviceListener(JetstreamEvent event, RetryEventCode code, String msg) {
		try {
			if (m_adviceListener != null) {
				if (event.containsKey(JetstreamReservedKeys.RetryCount.toString())) {
					Integer retryCount = (Integer) event.get(JetstreamReservedKeys.RetryCount.toString());
					if (retryCount > 3) {
						LOGGER.info( "Unable to deliver this event so dropping it.." + event.getEventId());
						incrementEventDroppedCounter();
						return;
					}
				}
				m_adviceListener.retry(event, code, msg);
				m_eventSentToAdviceListener.increment();
			}
		}
		catch (Throwable e) {
			m_errors.registerError(e);
			LOGGER.error( e.getLocalizedMessage(), e);
		}
	}

	/**
	 * @param address
	 */
	public void setAddress(ChannelAddress address) {
		m_address = (RESTChannelAddress) address;
	}

	/**
	 * @param advisoryListener
	 *          the advisoryListener to set
	 */
	public void setAdviceListener(Advice advisoryListener) {
		m_adviceListener = advisoryListener;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.channel.OutboundChannel#setAlarmListener(com.ebay.jetstream.event.channel.ChannelAlarmListener
	 * )
	 */
	public void setAlarmListener(ChannelAlarmListener alarmListener) {
		m_channelAlarmListener = alarmListener;
	}

	/**
	 * @param config
	 *          the config to set
	 */
	public void setConfig(HttpClientConfig config) {
		m_config = config;
	}

	/**
	 * @param numConnections
	 *          the numConnections to set
	 */

	public void setConstructEventHolder(boolean constructEventHolder) {
		m_constructEventHolder = constructEventHolder;
	}

	/**
	 * 
	 * Sets the rootcontext in hashmap to true indicating there is an alarm on this topic
	 * 
	 * @param rootContext
	 */
	private void setTopicAlarmState(String rootContext) {
		m_alarmState.get(rootContext).set(true);
	}

	public void shutDown() {
		m_batchPublisher.shutdown();
		close();
		int i = 0;
		while (m_queueProcessor.isAlive()) {
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				// swallow
			}

			if (i++ > 10)
				break; // we will give up after 10 secs

		}
		m_client.shutdown();
	}

	@Override
	public String toString() {

		return getBeanName();
	}

	
	@Override
	public void processApplicationEvent(ApplicationEvent event) {
		// TODO Auto-generated method stub
		
	}

	public void incrementDropCounter() {
		incrementEventDroppedCounter();
		
	}

	public void incrementSendCounter() {
		incrementEventSentCounter();		
	}
	
}
