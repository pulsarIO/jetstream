/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging;

/**
 * A InboundMessagingChannel represents messaging channel deployed at the ingress
 * of an application. It is provided an address that contains a list of Jetstream
 * Message service topics to subscribe to. It receives events from the Message Service
 * and delivers to it's deplyed eventSinks. It also handles pause and resume
 * conditions as part of which it will unsubscribe and subscribe to the
 * configured Jetstream Topics
 *
 * @author shmurthy@ebay.com
 *
 */
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.channel.AbstractInboundChannel;
import com.ebay.jetstream.event.channel.ChannelAddress;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.messagetype.Any;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.notification.AlertListener.AlertStrength;


@ManagedResource(objectName = "Event/Channel", description = "Inbound messaging component")
public class InboundMessagingChannel extends AbstractInboundChannel implements
		IMessageListener {
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.channel.messaging");
	
	enum Signal { PAUSE, RESUME };
	
	private static final String LOGGING_NAME = "InboundMessagingChannel";

	private int m_maxPauseQueueSize = 300;

	private int m_pauseQueueDrainRate = 1000; // 20 per sec

	private MessagingChannelAddress m_address;
	private Advice m_adviceListener;

	// cache to store received events once we have received a pause. The cache
	// will be emptied on
	// reception of resume
	private final ConcurrentLinkedQueue<JetstreamEvent> m_pauseCache = new ConcurrentLinkedQueue<JetstreamEvent>();

	private final LongCounter m_eventSentToAdviceListener = new LongCounter();
	private AtomicBoolean m_channelOpened = new AtomicBoolean(false);

	private int m_shutDownEventsSent;

	private final AtomicBoolean m_shutdownStatus = new AtomicBoolean(false);
	private long m_waitTimeBeforeShutdown = 5000;

    public long getWaitTimeBeforeShutdown() {
		return m_waitTimeBeforeShutdown;
	}

	public void setWaitTimeBeforeShutdown(long m_waitTimeBeforeShutdown) {
		this.m_waitTimeBeforeShutdown = m_waitTimeBeforeShutdown;
	}

	public InboundMessagingChannel() {
	}

	public void afterPropertiesSet() throws Exception {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#close()
	 */
	@Override
	public void close() throws EventException {

		if (!m_channelOpened.get())
			return;

		super.close();

		LOGGER.info( "Closing Inbound Messaging Channel");

		Management.removeBeanOrFolder(getBeanName(), this);
		m_eventsReceivedPerSec.destroy();
		m_eventsSentPerSec.destroy();

		List<JetstreamTopic> channelTopics = m_address
				.getChannelJetstreamTopics();

		for (JetstreamTopic topic : channelTopics) {
			try {
				unSubscribe(topic);
			} catch (Throwable e) {
				registerError(e);
				throw new EventException(e.getMessage());
			}
		}

		m_channelOpened.set(false);
	}

	private void fireEvent(JetstreamEvent evt) {
		super.fireSendEvent(evt);
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
		return m_address;
	}

	public Advice getAdviceListener() {
		return m_adviceListener;
	}

	@Override
	public int getPendingEvents() {

		return m_pauseCache.size();
	}

	/**
	 * 
	 * @return
	 */

	public long getEventSentToAdviceListener() {
		return m_eventSentToAdviceListener.get();
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessageListener#onMessage(com.ebay
	 * .jetstream.messaging.JetstreamMessage)
	 */
	public void onMessage(JetstreamMessage m) {

		if (m instanceof Any && ((Any) m).getObject() instanceof JetstreamEvent) {

			incrementEventRecievedCounter();

			JetstreamEvent event = (JetstreamEvent) ((Any) m).getObject();

			if (LOGGER.isDebugEnabled())
				LOGGER.debug( event.toString());

			// if we are paused then we will put in our pause cache else forward
			// the event downstream

			if (isPaused() && !m_shutdownStatus.get()) {
				if (m_pauseCache.size() < m_maxPauseQueueSize) {
					m_pauseCache.offer(event);
				} else {
					incrementEventDroppedCounter();
				}
			} else {
				try {
					fireEvent(event);
					incrementEventSentCounter();
					setLastEvent(event);
				} catch (EventException exe) {
					sendToAdviceListener(event, RetryEventCode.PAUSE_RETRY,
							exe.getLocalizedMessage());
					incrementEventDroppedCounter();
				} catch (Throwable t) {
					registerError(t);
					incrementEventDroppedCounter();
					LOGGER.warn( t.getLocalizedMessage());
					return;
				}
				// now see if pause cache has some elements to be sent
				// downstream.
				while (!m_pauseCache.isEmpty()) {
					event = m_pauseCache.poll();
					try {
						fireEvent(event);
						incrementEventSentCounter();
					} catch (Throwable t) {
						sendToAdviceListener(event, RetryEventCode.PAUSE_RETRY,
								t.getLocalizedMessage());
						registerError(t);
						LOGGER.warn( t.getLocalizedMessage());
						incrementEventDroppedCounter();
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
	@Override
	public void open() throws EventException {

		if (m_channelOpened.get())
			return;

		super.open();

		LOGGER.info( "Opening Inbound Messaging Channel");

		Management.removeBeanOrFolder(getBeanName(), this);
		Management.addBean(getBeanName(), this);

		List<JetstreamTopic> channelTopics = m_address
				.getChannelJetstreamTopics();

		for (JetstreamTopic topic : channelTopics) {
			try {
				subscribe(topic);
			} catch (Throwable t) {
				registerError(t);
				LOGGER.error(
						"Error Opening Inbound Messaging Channel"
								+ t.getLocalizedMessage());
			}
		}
		m_channelOpened.set(true);
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
			LOGGER.warn( "InboundMessagingChannel "
					+ getBeanName()
					+ " could not be paused. It is already in paused state",
					"InboundMessagingChannel" + ".pause()");
			return;
		}

		notifyProducer(Signal.PAUSE);
		
		changeState(ChannelOperationState.PAUSE);

	}

    void notifyProducer(Signal signal) {
        List<JetstreamTopic> channelTopics = m_address
				.getChannelJetstreamTopics();

		for (JetstreamTopic topic : channelTopics) {
			try {
			    if (Signal.PAUSE.equals(signal)) {
			        MessageService.getInstance().pause(topic);
			    } else {
			        MessageService.getInstance().resume(topic);
			    }
			} catch (Throwable e) {
				registerError(e);
				throw new EventException(e.getMessage());
			}
		}
		
		if (Signal.PAUSE.equals(signal)) {
		    postAlert(this.getBeanName() + " is being paused ", AlertStrength.RED);

		    LOGGER.warn( " Inbound Messaging Channel paused.");
		} else {
		    postAlert(getBeanName() + " is being resumed", AlertStrength.YELLOW);
	        LOGGER.warn( "Resuming Inbound Messaging Channel");
		}
    }

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {

			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

			// Calculate changes
			if (bcInfo.isChangedBean(getAddress())) {

				LOGGER.info(
						"Received new configuration for InboundMessageChannel - "
								+ getBeanName());

				try {

					close();
				} catch (Throwable e) {

					registerError(e);
					LOGGER.error(
							"Error closing InboundMessagingChannel while applying new config - "
									+ e.getMessage());

				}
				setAddress((ChannelAddress) bcInfo.getChangedBean());
				try {
					open();
				} catch (Throwable e) {
					registerError(e);
					// swallow the exception and log

					LOGGER.error(
							"Error opening InboundMessagingChannel while applying new config - "
									+ e.getMessage());

				}
			}
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
			LOGGER.warn( "InboundMessagingChannel "
					+ getBeanName()
					+ " could not be resumed. It is already in resume state",
					"InboundMessagingChannel" + ".pause()");
			return;
		}

		// if we are in paused state then first empty the pause Cache which
		// could have received events
		// that were in the pipeline when we unsubscribed in response to a
		// pause.

		if (isPaused() && !m_shutdownStatus.get()) {

			changeState(ChannelOperationState.RESUME);

			while (!m_pauseCache.isEmpty()) {

				JetstreamEvent event = m_pauseCache.poll();

				try {
					fireEvent(event);
					setLastEvent(event);
					incrementEventSentCounter();

				} catch (Throwable t) {
					registerError(t);
					incrementEventDroppedCounter();
				}
				// throttle the drain of the pause queue here so our sink is not
				// swamped resulting in a pause again
				try {
					Thread.sleep(1000 / m_pauseQueueDrainRate);
				} catch (InterruptedException e) {

				}
			}

			notifyProducer(Signal.RESUME);
		}

	}

	private void sendToAdviceListener(JetstreamEvent event,
			RetryEventCode code, String msg) {
		if (m_adviceListener != null) {
			m_adviceListener.retry(event, code, msg);
			incrementAdviceListenerCount();
		}
	}

	/**
	 * @param address
	 */
	public void setAddress(ChannelAddress address) {
		m_address = (MessagingChannelAddress) address;
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

	@Override
	public void shutDown() {
		
		m_shutdownStatus.set(true);
		
		// now issue a pause to message service.
		// this will allow traffic to flow in.
		
		notifyProducer(Signal.PAUSE);
		
		try {
			Thread.sleep(m_waitTimeBeforeShutdown);
		} catch (InterruptedException e) {
			
		}
		
		if (m_pauseCache != null) {
			JetstreamEvent event = null;
			while (!m_pauseCache.isEmpty()) {
				event = m_pauseCache.poll();
				try {
					fireEvent(event);
					incrementEventSentCounter();
				} catch (EventException e) {
					registerError(e, event);
					sendToAdviceListener(event, RetryEventCode.PAUSE_RETRY,
							e.getMessage());
				} catch (Throwable t) {
					registerError(t);
					LOGGER.error( t.getLocalizedMessage());
				}
				m_shutDownEventsSent++;
			}
			
					
			close();
			
			LOGGER.warn( 
							m_shutDownEventsSent
									+ " events drained from its queue due to graceful shutdown - " +
									"final events received = " + getTotalEventsReceived() +
									"final events sent = " + getTotalEventsSent() +
									"final total events dropped =" + getTotalEventsDropped(), 
							LOGGING_NAME);

		}
	}

	/**
	 * @param topic
	 * @throws Exception
	 */
	private void subscribe(JetstreamTopic topic) throws Exception {

		try {

			LOGGER.info(
					"Subscribing to Topic - " + topic.getTopicName()
							+ " on Inbound Messaging Channel");

			MessageService.getInstance().subscribe(topic, this);
		} catch (MessageServiceException e) {

			registerError(e);
			LOGGER.error(
					"Error Subscribing for Topic - " + topic.getTopicName()
							+ e.getMessage());

		} catch (Throwable e) {

			registerError(e);
			LOGGER.error(
					"Error Subscribing for Topic - " + topic.getTopicName()
							+ e.getMessage());

		}

	}

	/**
	 * @param topic
	 * @throws Exception
	 */
	private void unSubscribe(JetstreamTopic topic) throws Exception {

		try {

			LOGGER.info(
					"Unsubscribing to Topic - " + topic.getTopicName()
							+ " on Inbound Messaging Channel");

			MessageService.getInstance().unsubscribe(topic, this);
		} catch (MessageServiceException e) {

			registerError(e);
			LOGGER.error(
					"Error Subscribing for Topic - " + topic.getTopicName()
							+ e.getMessage());

		} catch (Exception e) {
			registerError(e);
			LOGGER.error(
					"Error Subscribing for Topic - " + topic.getTopicName()
							+ e.getMessage());

		}
	}
}
