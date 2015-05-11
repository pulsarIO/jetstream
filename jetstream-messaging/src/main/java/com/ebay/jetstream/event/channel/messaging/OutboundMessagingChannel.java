/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging;

/**
 * A OutboundMessagingChannel represents messaging channel deployed at the egress
 * of an application. It is provided an address that contains a list of Jetstream
 * Message service topics to publish to. It receives events from it's event source
 * and publishes the same on the message bus using Jetstream Message Service.
 * It subscribes to Message Service internal state advisory. The advisory coming
 * out of the message service provides it with stop sending and resume advise
 * messages. In response to stop sending message it raises an alarm signalling
 * overrun condition and upon receving resume advise from message service it
 * clears the alarm. When a JetstreamEvent arrives to be sent on the bus, it is
 * examined to see if it contains affinityKeys. If it does, the affinity key is
 * extracted, the JetstreamEvent is inserted in to a Jetstream Message of type any,
 * the affinity key set on the message and subbsequently the message is sent
 * to message service to be send out on the bus.
 *
 * @author shmurthy@ebay.com
 *
 */

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.application.JetstreamApplication;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.channel.AbstractOutboundChannel;
import com.ebay.jetstream.event.channel.ChannelAddress;
import com.ebay.jetstream.event.channel.ChannelAlarm;
import com.ebay.jetstream.event.support.ErrorManager;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.messagetype.AdvisoryMessage;
import com.ebay.jetstream.messaging.messagetype.AdvisoryMessage.AdvisoryCode;
import com.ebay.jetstream.messaging.messagetype.Any;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.notification.AlertListener.AlertStrength;

@ManagedResource(objectName = "Event/Channel", description = "Outbound messaging component")
public class OutboundMessagingChannel extends AbstractOutboundChannel implements IMessageListener {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.channel.messaging");

	private final static String affinityKey = JetstreamReservedKeys.MessageAffinityKey.toString();
	private final static String bcastMsg = JetstreamReservedKeys.EventBroadCast.toString();

	private final AtomicBoolean m_alarmRaised = new AtomicBoolean(false);

	private MessagingChannelAddress m_address;
	private final LongCounter m_retryAdvisoryEvents = new LongCounter();


	private final ConcurrentHashMap<String, AtomicBoolean> m_alarmState = new ConcurrentHashMap<String, AtomicBoolean>();
	private final AtomicLong m_eventSentToAdviceListener = new AtomicLong(0);
	private boolean m_constructEventHolder = false;
	private AtomicBoolean m_channelOpened = new AtomicBoolean(false);
	private ErrorManager m_errors = new ErrorManager();
	private int retryCount = 3;


	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

	public OutboundMessagingChannel() {
	}

	public void afterPropertiesSet() throws Exception {

		List<JetstreamTopic> publishingTopics = m_address.getChannelJetstreamTopics();

		for (JetstreamTopic topic : publishingTopics) {

			MessageService.getInstance().prepareToPublish(topic);
		}

	}

	public ErrorManager getErrors() {
		return m_errors;
	}

	private boolean areAllContextsInAlarmCondition() {
		Iterator<Entry<String, AtomicBoolean>> itr = m_alarmState.entrySet().iterator();

		while (itr.hasNext()) {
			Entry<String, AtomicBoolean> entry = itr.next();
			if (!entry.getValue().get())
				return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#close()
	 */
	public void close() throws EventException {

		LOGGER.info( "Closing Outbound Messaging Chanel");

		if (!m_channelOpened.get()) return;

		super.close();

		Management.removeBeanOrFolder(getBeanName(), this);

		m_channelOpened.set(false);

		if (getAdviceListener() != null) {
			List<JetstreamTopic> publishingTopics = m_address
					.getChannelJetstreamTopics();

			for (JetstreamTopic topic : publishingTopics) {

				try {
					MessageService.getInstance().unsubscribe(
							new JetstreamTopic(topic.getRootContext()
									+ "/InternalStateAdvisory"), this);
				} catch (MessageServiceException e) {
					m_errors.registerError(e);
					String errMsg = "Error Unsubscribing for Topic - " + topic.getTopicName();
					errMsg += " - ";
					errMsg += e.getMessage();

					LOGGER.error( errMsg);

				}
				catch (Throwable e) {
					m_errors.registerError(e);
					LOGGER.error( "Error unSubscribing for Topic - " + topic.getTopicName() + e.getMessage());
				}
			}
		}

	}

	private JetstreamEvent constructTEventHolder(JetstreamEvent event) {
		if (event != null) {
			JetstreamEvent holderEvent = new JetstreamEvent();
			holderEvent.put(JetstreamReservedKeys.JetstreamEventHolder.toString(), event);
			holderEvent.put(JetstreamReservedKeys.EventType.toString(), event.getEventType());
			// insert source application to DTE
			String eventSource = null;
			if (event.get(JetstreamReservedKeys.EventSource.toString()) != null) {
				eventSource = (String) event.get(JetstreamReservedKeys.EventSource.toString());
			}
			else {
				eventSource = JetstreamApplication.getInstance().getApplicationInformation().getApplicationName();
			}

			holderEvent.put(JetstreamReservedKeys.EventSource.toString(), eventSource);

			return holderEvent;
		}
		return event;
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

	public long getRetryAdvisoryEvents() {
		return m_retryAdvisoryEvents.get();
	}

	private void incrementAdviceListenerCount() {
		if (m_eventSentToAdviceListener.incrementAndGet() < 0) {
			m_eventSentToAdviceListener.set(0);
		}
	}

	private void incrementRetryAdvisoryEvents() {
		m_retryAdvisoryEvents.increment();
	}

	public boolean isConstructEventHolder() {
		return m_constructEventHolder;
	}

	/**
	 * @param context
	 * @return
	 */
	private boolean matchesMyPublishingContexts(String context) {
		List<JetstreamTopic> publishingTopics = m_address.getChannelJetstreamTopics();

		for (JetstreamTopic topic : publishingTopics) {
			if (context.equals(topic.getRootContext()))
				return true;
		}
		return false;
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.messaging.JetstreamMessageListener#onMessage(com.ebay.jetstream.messaging.JetstreamMessage)
	 */
	public void onMessage(JetstreamMessage m) {
		if (m instanceof AdvisoryMessage) {
			AdvisoryMessage advise = (AdvisoryMessage) m;

			if (advise.isStopSending()
					&& matchesMyPublishingContexts(advise.getAdvisoryTopic()
							.getRootContext())) {
				LOGGER.warn(
						"Outbound Messaging Channel raising alarm");
				setTopicAlarmState(advise.getAdvisoryTopic().getRootContext());

				incrementAlarmsRaisedCounter();

				if (areAllContextsInAlarmCondition()) {
					m_alarmRaised.set(true);
					getAlarmListener().alarm(ChannelAlarm.OVERRUN);
					postAlert("All Contexts in Alarm Condition - ", AlertStrength.ORANGE);

				}
			} else if (advise.isResumeSending()
					&& matchesMyPublishingContexts(advise.getAdvisoryTopic()
							.getRootContext())) {
				LOGGER.info(
						"Outbound Messaging Channel clearing alarm for topic-> "
								+ advise.getAdvisoryTopic().getRootContext());

				resetTopicAlarmState(advise.getAdvisoryTopic().getRootContext());

				if (!areAllContextsInAlarmCondition()) {
					m_alarmRaised.set(false);
					getAlarmListener().alarm(ChannelAlarm.CLEAR);
					postAlert("Alarm Condition Cleared - ", AlertStrength.YELLOW);
					incrementAlarmsClearedCounter();
				}
			}
			else if (advise.isResendMessage()) {

				JetstreamEvent evt = (JetstreamEvent) ((Any) ((AdvisoryMessage) m)
						.getUndeliveredMsg()).getObject();

				try {
					evt = evt.clone();
				} catch (CloneNotSupportedException e) {
					m_errors.registerError(e);
					LOGGER.debug( "Failed to clone event : Exception = " + e.getLocalizedMessage());
					this.incrementEventDroppedCounter();
					return;
				}

				List<String> topics = m_address.getChannelTopics();
				String topic = ((AdvisoryMessage) m).getAdvisoryTopic().toString();
				// Fix issue when multiple OMCs subscribe different topics under same context.
				// Each OMC will receive broadcast advisory events.
				if (topic == null || topics.contains(topic)) {
					evt.put(JetstreamReservedKeys.EventReplayTopic.toString(), topic);

					Integer retryCount = 1;
					if (evt != null) {
						if (evt.containsKey(JetstreamReservedKeys.RetryCount
								.toString())) {
							retryCount = (Integer) evt
									.get(JetstreamReservedKeys.RetryCount
											.toString());
							retryCount++;
						}
						evt.put(JetstreamReservedKeys.RetryCount.toString(),
								retryCount);
						sendToAdviceListener(evt, RetryEventCode.MSG_RETRY,
								AdvisoryCode.RESEND_MESSAGE.toString());
						incrementRetryAdvisoryEvents();
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

		if (m_channelOpened.get()) return;

		super.open();

		LOGGER.info( "Opening Outbound Messaging Channel");

		Management.removeBeanOrFolder(getBeanName(), this);
		Management.addBean(getBeanName(), this);

		if (getAdviceListener() != null) {
			List<JetstreamTopic> publishingTopics = m_address
					.getChannelJetstreamTopics();

			for (JetstreamTopic topic : publishingTopics) {

				subscribe(new JetstreamTopic(topic.getRootContext()
						+ "/InternalStateAdvisory")); // subscribe to advisory
				// messages
			}
		}

		m_channelOpened.set(true);
	}



	/**
	 * 
	 */
	@ManagedOperation
	public void resetStats() {
		super.resetStats();
		m_retryAdvisoryEvents.set(0);		
	}

	/**
	 * 
	 * Resets the rootcontext in hashmap to false indicating alarm has been cleared on the topic
	 * 
	 * @param rootContext
	 */
	private void resetTopicAlarmState(String rootContext) {
		try {
			if (rootContext != null) {
				AtomicBoolean alarmState = m_alarmState.get(rootContext);
				if (alarmState != null)
					alarmState.set(false);
			}
		} catch (Exception e) {
			// if root context does not exist we will get NPE - we will swallow it.
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.EventSink#sendEvent(com.ebay.jetstream.event.JetstreamEvent)
	 */
	public void sendEvent(JetstreamEvent event) throws EventException {


		incrementEventRecievedCounter();

		List<JetstreamTopic> publishingTopics = m_address.getChannelJetstreamTopics();

		String[] forwardingTopics = event.getForwardingTopics();


		for (JetstreamTopic topic : publishingTopics) {

			if (forwardingTopics != null) {
				boolean bFound = false;
				String strSeekName = topic.getTopicName();
				for (String strTopic : forwardingTopics) {
					if (strTopic.equals(strSeekName)) {
						bFound = true;
						break;
					}
				}
				if (!bFound)
					continue;
			}

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug( "eventSource=" + getBeanName() + "&eventId = " + event.getEventId() + "&topic="
						+ topic);
			}

			Any any = null;
			if (isConstructEventHolder()) {
				JetstreamEvent holderEvent = constructTEventHolder(event);
				any = new Any(holderEvent);
			}
			else {
				any = new Any(event);
			}

			any.setPriority(JetstreamMessage.HI_PRIORITY);

			if (event.containsKey(affinityKey)) {

				Object affkey = event.get(affinityKey);

				if (affkey == null) {
					incrementEventDroppedCounter();
					if (LOGGER.isDebugEnabled()) 
						LOGGER.debug(" Affinity Key passed in is null");
					return;
				}

				any.setAffinityKey(affkey);

				if (LOGGER.isDebugEnabled()) {

					LOGGER.debug( "sending on topic - " + topic.getTopicName() + " " + ": Affinity Key = "
							+ event.get(affinityKey).toString() + " EVENT - " + event.toString());

				}

			}

			// we assume that caller will not set affinity key and bcast flag at the same time
			// if affinity key is set it will override bcast

			if (event.getMetaData(bcastMsg) != null) {

				any.setBroadcastMessage(Boolean.parseBoolean((String) event.getMetaData(bcastMsg)));

				if (LOGGER.isDebugEnabled()) {

					LOGGER.debug( "sending on topic - " + topic.getTopicName() + " " + ": BroadCast Key = "
							+ event.getMetaData(bcastMsg).toString() + " EVENT - " + event.toString());
				}

			}
			try {
				MessageService.getInstance().publish(topic, any);
				setLastEvent(event);
				incrementEventSentCounter();
			}
			catch (MessageServiceException mse) {

				m_errors.registerError(mse);

				String errMsg = "Error Publishing Message for Topic - " + topic.getTopicName(); // NOPMD
				errMsg += " - ";
				errMsg += mse.getMessage();

				if (LOGGER.isWarnEnabled()) {
					LOGGER.warn( errMsg);
				}
				sendToAdviceListener(event, RetryEventCode.UNKNOWN, errMsg);

				incrementEventDroppedCounter();

				if (mse.getError() == MessageServiceException.BUFFER_FULL && !m_alarmRaised.get()) {
					setTopicAlarmState(topic.getRootContext());
					if (areAllContextsInAlarmCondition()) {
						getAlarmListener().alarm(ChannelAlarm.OVERRUN);
						postAlert("MessageService Buffer full condition - ", AlertStrength.YELLOW);
						LOGGER.info( "Outbound Messaging Channel raising alarm");
						m_alarmRaised.set(true);
						incrementAlarmsRaisedCounter();
					}
				}

			}
			catch (Throwable t) {
				m_errors.registerError(t);
				String errMsg = "Error Publishing Message for Topic - " + topic.getTopicName();
				errMsg += " - ";
				errMsg += t.getMessage();
				sendToAdviceListener(event, RetryEventCode.UNKNOWN, errMsg);
				incrementEventDroppedCounter();
				if (LOGGER.isDebugEnabled()) 
					LOGGER.debug( errMsg);

			}
		}
	}

	/**
	 * @param event
	 * @param code
	 * @param msg
	 */
	private void sendToAdviceListener(JetstreamEvent event, RetryEventCode code, String msg) {
		try {
			if (getAdviceListener() != null) {
				if (event.containsKey(JetstreamReservedKeys.RetryCount.toString())) {
					Integer retryCount = (Integer) event.get(JetstreamReservedKeys.RetryCount.toString());
					if (retryCount > getRetryCount()) {
						LOGGER.info( "Unable to deliver this event so dropping it.." + event.getEventId());
						m_totalEventsDropped.increment();
						return;
					}
				}
				getAdviceListener().retry(event, code, msg);
				incrementAdviceListenerCount();
			}
		}
		catch (Throwable e) {
			m_errors.registerError(e);
			LOGGER.debug( e.getLocalizedMessage());
		}
	}

	/**
	 * @param address
	 */
	public void setAddress(ChannelAddress address) {
		if (m_address != null) {
			m_address.setChannelTopics(((MessagingChannelAddress) address).getChannelTopics());
		}
		else
			m_address = (MessagingChannelAddress) address;

		m_alarmState.clear();
		for (JetstreamTopic topic : m_address.getChannelJetstreamTopics()) {
			m_alarmState.put(topic.getRootContext(), new AtomicBoolean(false));
		}
	}



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
		try {
			if (rootContext != null) {
				AtomicBoolean alarmState = m_alarmState.get(rootContext);
				if (alarmState != null)
					alarmState.set(true);
			}

		} catch (Exception e) {
			m_errors.registerError(e);
			// we might get a NPE if rootContext is not in map - swallow it
		}
	}

	public void shutDown() {
		close();
	}

	/**
	 * @param topic
	 */
	private void subscribe(JetstreamTopic topic) {
		try {

			LOGGER.info( "Subscribing to Topic - " + topic.getTopicName() + " on Outbound Messaging Channel");

			MessageService.getInstance().subscribe(topic, this);
		}
		catch (MessageServiceException e) {
			m_errors.registerError(e);
			LOGGER.error( "Error Subscribing for Topic - " + topic.getTopicName() + e.getMessage());

		}
		catch (Exception e) {
			m_errors.registerError(e);
			LOGGER.error( "Error Subscribing for Topic - " + topic.getTopicName() + e.getMessage());
		}
	}

	@Override
	public String toString() {

		return getBeanName();
	}


	@Override
	public void processApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {
			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;
			// Calculate changes
			if (bcInfo.isChangedBean(getAddress())) {
				setAddress((ChannelAddress) bcInfo.getChangedBean());
			}
		}

	}

	@Override
	public int getPendingEvents() {

		return 0;
	}

}
