/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging.http.inbound;


import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.channel.AbstractInboundChannel;
import com.ebay.jetstream.event.channel.ChannelAddress;
import com.ebay.jetstream.http.netty.server.HttpServer;
import com.ebay.jetstream.http.netty.server.HttpServerConfig;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.Hidden;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

@SuppressWarnings("rawtypes")
@ManagedResource(objectName = "Event/Channel", description = "Inbound REST Channel")
public class InboundRESTChannel extends AbstractInboundChannel 
{
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.http.inbound");

	private int m_maxPauseQueueSize = 300;

	private int m_pauseQueueDrainRate = 1000; // 20 per sec

	private Advice m_adviceListener;

	// cache to store received events once we have received a pause. The cache will be emptied on
	// reception of resume
	private final ConcurrentLinkedQueue<JetstreamEvent> m_pauseCache = new ConcurrentLinkedQueue<JetstreamEvent>();
		
	private int m_shutDownEventsSent;

	private final AtomicBoolean m_shutdownStatus = new AtomicBoolean(false);

	private HttpServer m_server;

	private HttpServerConfig m_serverConfig = new HttpServerConfig();
	
	private String m_servletPath = "/RTBDEventConsumerServlet";

	private InboundRTBDEventServlet m_servlet = new InboundRTBDEventServlet(this);
	
	

	public InboundRESTChannel() {
	}

	public void afterPropertiesSet() throws Exception {

		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#close()
	 */
	public void close() throws EventException {

		super.close();
		
		Management.removeBeanOrFolder(getBeanName(), this);
			
		m_server.shutDown();
	
		LOGGER.info( "Closing Inbound Messaging Channel");

	}

		
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#flush()
	 */
	public void flush() throws EventException {
		// This is NOOP for us

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#getAddress()
	 */
	public ChannelAddress getAddress() {
		return null;
	}

	public Advice getAdviceListener() {
		return m_adviceListener;
	}


	@Hidden
	@Override
	public int getPendingEvents() {

		return m_pauseCache.size();
	}

	
	public int getMaxPauseQueueSize() {
		return m_maxPauseQueueSize;
	}

		
	public int getPauseQueueDrainRate() {
		return m_pauseQueueDrainRate;
	}

	/**
	 * @return the pauseQueueSize
	 */
	public long getPauseQueueSize() {
		return m_pauseCache.size();
	}

	
	/**
	 * @return the server
	 */
	@Hidden
	public HttpServer getServer() {
		return m_server;
	}
	
	/**
	 * @return the server
	 */
	public void setServer(HttpServer server) {
		m_server = server;
	}
	
	

	/**
	 * @return the serverConfig
	 */
	@Hidden
	public HttpServerConfig getServerConfig() {
		return m_serverConfig;
	}

	public boolean getShutdownStatus() {
		return m_shutdownStatus.get();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.messaging.JetstreamMessageListener#onMessage(com.ebay.jetstream.messaging.JetstreamMessage)
	 */
	public void onMessage(JetstreamEvent event) {

		if (event != null) {

			incrementEventRecievedCounter();

			if (LOGGER.isDebugEnabled())
				LOGGER.debug( event.toString());

			// if we are paused then we will put in our pause cache else forward the event downstream

			if (m_isPaused.get() && !m_shutdownStatus.get()) {
				if (m_pauseCache.size() < m_maxPauseQueueSize) {
					m_pauseCache.offer(event);
				}
				else {
					sendToAdviceListener(event, RetryEventCode.QUEUE_FULL, "Queue got filled");
	
					incrementEventDroppedCounter();
				}
			}
			else {
				try {

					super.fireSendEvent(event);
				    setLastEvent(event);
					incrementEventSentCounter();

				}
				catch (Throwable t) {
					registerError(t, event);
					sendToAdviceListener(event, RetryEventCode.PAUSE_RETRY, t.getLocalizedMessage());
					incrementEventDroppedCounter();
					LOGGER.warn( t.getLocalizedMessage());
				} 
				
				
				// now see if pause cache has some elements to be sent downstream.
				while (!m_pauseCache.isEmpty()) {
					event = m_pauseCache.poll();
					try {
						super.fireSendEvent(event);
						setLastEvent(event);
						incrementEventSentCounter();
					}
					catch (Throwable t) {
						registerError(t, event);
						sendToAdviceListener(event, RetryEventCode.PAUSE_RETRY, t.getLocalizedMessage());
						incrementEventDroppedCounter();
						LOGGER.warn( t.getLocalizedMessage());
					}
					
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#open()
	 */
	public void open() throws EventException {

		super.open();
		
		Management.removeBeanOrFolder(getBeanName(), this);
		Management.addBean(getBeanName(), this);
	
		m_servlet.setInboundChannel(this);
		m_server.add(m_servletPath, m_servlet);
		
		LOGGER.info( "Opening Inbound Messaging Channel");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.InboundChannel#pause()
	 */
	@Override
	@ManagedOperation
	public void pause() throws EventException {
		if (isPaused()) {
			LOGGER.error( "InboundMessagingChannel " + getBeanName()
					+ " could not be paused. It is already in paused state", "InboundMessagingChannel" + ".pause()");
			return;
		}

		changeState(ChannelOperationState.PAUSE);
		
		m_server.shutDown();
		
		LOGGER.error( " Inbound Messaging Channel paused.");

	}

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {

			// nothing to do for this component as the HttpServer will handle it's config change
	
		}

	}

	
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.InboundChannel#resume()
	 */
	
	@Override
	@ManagedOperation
	public void resume() throws EventException {
		if (!isPaused()) {
			LOGGER.error( "InboundMessagingChannel " + getBeanName()
					+ " could not be resumed. It is already in resume state", "InboundMessagingChannel" + ".pause()");
			return;
		}

		// if we are in paused state then first empty the pause Cache which could have received events
		// that were in the pipeline when we unsubscribed in response to a pause.
		if (isPaused() && !m_shutdownStatus.get()) {

			changeState(ChannelOperationState.RESUME);
				
			while (!m_pauseCache.isEmpty()) {

				JetstreamEvent event = m_pauseCache.poll();

				incrementEventSentCounter();
				
				try {
					super.fireSendEvent(event);
					setLastEvent(event);
					incrementEventSentCounter();
				} catch (Throwable t) {
					incrementEventDroppedCounter();
				}
				
				// throttle the drain of the pause queue here so our sink is not swamped resulting in a pause again
				try {
					Thread.sleep(1000 / m_pauseQueueDrainRate);
				}
				catch (InterruptedException e) {

				}
			}
			
							
			try {
				m_server.start();
			} catch (Exception e) {
				registerError(e);
				LOGGER.error( "Failed to start server during Resume" + e.getLocalizedMessage());

			}
			
			LOGGER.warn( "Resuming Inbound Messaging Channel");

		}
		
	

	}

	private void sendToAdviceListener(JetstreamEvent event, RetryEventCode code, String msg) {
		if (m_adviceListener != null) {
			m_adviceListener.retry(event, code, msg);
			m_eventSentToAdviceListener.increment();
		}
	}

	/**
	 * @param address
	 */
	public void setAddress(ChannelAddress address) {

	}

	public void setAdviceListener(Advice adviceListener) {
		m_adviceListener = adviceListener;
	}

	public void setMaxPauseQueueSize(int maxPauseQueueSize) {
		m_maxPauseQueueSize = maxPauseQueueSize;
	}

	public void setPauseQueueDrainRate(int pauseQueueDrainRate) {
		m_pauseQueueDrainRate = pauseQueueDrainRate;
	}

	/**
	 * @param serverConfig
	 *          the serverConfig to set
	 */
	public void setServerConfig(HttpServerConfig serverConfig) {
		m_serverConfig = serverConfig;
	}
	
	/**
	 * @return
	 */
	public String getServletPath() {
		return m_servletPath;
	}

	/**
	 * @param servletPath
	 */
	public void setServletPath(String servletPath) {
		this.m_servletPath = servletPath;
	}
	
	/**
	 * @return
	 */
	
	@Hidden
	public InboundRTBDEventServlet getServlet() {
		return m_servlet;
	}

	/**
	 * @param m_servlet
	 */
	public void setServlet(InboundRTBDEventServlet servlet) {
		this.m_servlet = servlet;
	}
	

	@Override
	public void shutDown() {
		m_shutdownStatus.set(true);
		if (m_eventsReceivedPerSec != null)
			m_eventsReceivedPerSec.destroy();
		close();
		if (m_pauseCache != null) {
			JetstreamEvent event = null;
			while (!m_pauseCache.isEmpty()) {
				event = m_pauseCache.poll();
				try {
					super.fireSendEvent(event);
					setLastEvent(event);
					incrementEventSentCounter();
				}
				catch (EventException exception) {
					registerError(exception);
					sendToAdviceListener(event, RetryEventCode.PAUSE_RETRY, exception.getMessage());
				}
				catch (Throwable t) {
					registerError(t);
					LOGGER.error( t.getLocalizedMessage());
				}
				m_shutDownEventsSent++;
			}
		}
        LOGGER.warn( 
                m_shutDownEventsSent
                        + " events drained from its queue due to graceful shutdown - final events sent = " + getTotalEventsSent() +
                        "final total events dropped =" + getTotalEventsDropped() +
                        "final total events received =" + getTotalEventsReceived()); 
                

	}

}
