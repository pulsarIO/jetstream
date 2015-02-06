/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support;

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.application.JetstreamApplication;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.channel.InboundChannel;
import com.ebay.jetstream.event.support.channel.RemoteController;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.messagetype.MapMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.util.GuidGenerator;

/**
 * A generic advice listener, it have a configurable retry event codes, and it
 * just pass the retry event to the eventSinks configured to the processor.
 * 
 * If the retryEventCodes is null, all retry reasons will be passed to the
 * eventSinks.
 * 
 * The startReplay/stopRelay method will pause/resume inbound channels when configured,
 * it also send out the message when configured the replayNotificationTopic.
 * 
 * @author xingwang
 * 
 */
@ManagedResource(objectName = "Event/AdviceProcessor", description = "Advice processor")
public class AdviceProcessor extends AbstractEventProcessor implements Advice {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdviceProcessor.class.getPackage().getName());
    private static final String SERVER_ID = "" + GuidGenerator.gen() + "@"
            + JetstreamApplication.getInstance().getApplicationInformation().getApplicationName();
    private static final int HISTORY_COUNT = 32;
    
    private List<RetryEventCode> m_retryEventCodes;
    private List<InboundChannel> m_replayInboundChannels;
    private MessageService m_messageService;
    private String m_replayNotificationTopic;
    
    private List<String> m_cmdHistory = new ArrayList<String>(HISTORY_COUNT);
    private AtomicLong m_historySeq = new AtomicLong(0);
    
    private JetstreamEvent m_lastEvent;
    
    private final AtomicBoolean m_shutdownFlag = new AtomicBoolean(false);

    private final EnumMap<RetryEventCode, LongCounter> m_counters = new EnumMap<RetryEventCode, LongCounter>(
            RetryEventCode.class);

    private final ConcurrentMap<String, LongCounter> m_countersByTopic = new ConcurrentHashMap<String, LongCounter>();
    
    @Override
    public void abandon(JetstreamEvent event, int reasonCode, String reason) {
        // do nothing.
    }

    private void addToHistory(String msg) {
        synchronized (m_cmdHistory) {
            int location = (int)(m_historySeq.getAndIncrement() % HISTORY_COUNT);
            if (m_cmdHistory.size() < HISTORY_COUNT) {
                m_cmdHistory.add(new Date() + " : " + msg);
            } else {
                m_cmdHistory.set(location, new Date() + " : " + msg);
            }
        }
    }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        for (RetryEventCode v : RetryEventCode.values()) {
            m_counters.put(v, new LongCounter());
        }
        Management.addBean(getBeanName(), this);
        m_messageService = MessageService.getInstance();
    }

    @ManagedAttribute
    public List<String> getCmdHistory() {
        ArrayList<String> cmdHistory = new ArrayList<String>(HISTORY_COUNT);
        synchronized (m_cmdHistory) {
            long l = m_historySeq.get();
            if (l <= HISTORY_COUNT) {
                cmdHistory.addAll(m_cmdHistory);
            } else {
                long pos = l - HISTORY_COUNT;
                for (int i = 0; i < HISTORY_COUNT; i++) {
                    cmdHistory.add(m_cmdHistory.get((int) pos % HISTORY_COUNT));
                    pos ++;
                }
            }
        }
        return cmdHistory;
    }
    
    @Override
    public int getPendingEvents() {
        return 0;
    }
    
    public String getReplayNotificationTopic() {
        return m_replayNotificationTopic;
    }

    @ManagedAttribute
    public EnumMap<RetryEventCode, LongCounter> getRetryCountersByReason() {
        return m_counters;
    }

    @ManagedAttribute
    public Map<String, LongCounter> getRetryCountersByTopic() {
        return m_countersByTopic;
    }
    
    @ManagedAttribute
    public String getServerId() {
        return SERVER_ID;
    }

    @Override
    @ManagedOperation
    public void pause() {
        if (isPaused()) {
            return;
        }

        changeState(ProcessorOperationState.PAUSE);
    }

    @Override
    protected void processApplicationEvent(ApplicationEvent event) {
        // do nothing.
    }

    @Override
    @ManagedOperation
    public void resume() {
        if (isPaused()) {
            changeState(ProcessorOperationState.RESUME);
        }
    }

    @Override
    public void retry(JetstreamEvent event, RetryEventCode reasonCode, String reason) {
        // If retryEventCodes is null, retry all.
        if (m_retryEventCodes != null && !m_retryEventCodes.contains(reasonCode)) {
            super.incrementEventDroppedCounter();
            return;
        }

        m_counters.get(reasonCode).increment();

        sendEvent(event);
    }

    private void sendCommand(String command) {
        if (m_messageService.isInitialized()) {
            try {
                MapMessage msg = new MapMessage();
                msg.add(RemoteController.KEY_COMMAND, command);
                msg.add(RemoteController.KEY_SERVER, SERVER_ID);
                m_messageService.publish(new JetstreamTopic(m_replayNotificationTopic), msg);
            } catch (Exception e) {
                LOGGER.info( "Fatal Error : Publish method failed to broadcast the message", e);
            }
        } else {
            LOGGER.info( "Fatal Error : Message Service Not initialized");
        }
    }

    @Override
    public void sendEvent(JetstreamEvent event) throws EventException {
    	m_lastEvent = event;
    	String replayTopic = (String) event.get(JetstreamReservedKeys.EventReplayTopic.toString());
    	if (replayTopic != null) {
    	    LongCounter counter = m_countersByTopic.get(replayTopic);
    	    if (counter == null) {
    	        counter = new LongCounter();
    	        LongCounter existedCounter = m_countersByTopic.putIfAbsent(replayTopic, counter);
    	        if (existedCounter != null) {
    	            counter = existedCounter;
    	        }
    	    }
    	    counter.increment();
    	}
        super.incrementEventRecievedCounter();
        if (isPaused() || m_shutdownFlag.get()) {
            super.incrementEventDroppedCounter();
            return;
        }
        // Use this to avoid infinite loop.
        super.fireSendEvent(event);
        super.incrementEventSentCounter();
    }

	/**
	 * @return
	 */
	public JetstreamEvent getLastEvent() {
		return m_lastEvent;
	}
	
    public void setReplayInboundChannels(List<InboundChannel> replayInboundChannels) {
        this.m_replayInboundChannels = replayInboundChannels;
    } 

    public void setReplayNotificationTopic(String replayNotificationTopic) {
        this.m_replayNotificationTopic = replayNotificationTopic;
    }

    public void setRetryEventCodes(List<RetryEventCode> retryEventCodes) {
        this.m_retryEventCodes = retryEventCodes;
    }
    
    
    @Override
    public void shutDown() {
        m_shutdownFlag.compareAndSet(false, true);
    }
    

    @ManagedOperation
    @Override
    public void startReplay() {
        addToHistory(RemoteController.COMMAND_START);
        if (this.m_replayNotificationTopic != null) {
            sendCommand(RemoteController.COMMAND_START);
        }
        List<InboundChannel> channels = m_replayInboundChannels;
        if (channels != null) {
            for (InboundChannel c : channels) {
                c.resume();
            }
        }
    }

    @ManagedOperation
    @Override
    public void stopReplay() {
        addToHistory(RemoteController.COMMAND_STOP);
        if (this.m_replayNotificationTopic != null) {
            sendCommand(RemoteController.COMMAND_STOP);
        }
        List<InboundChannel> channels = m_replayInboundChannels;
        if (channels != null) {
            for (InboundChannel c : channels) {
                c.pause();
            }
        }
    }

    @Override
    public void success(JetstreamEvent event) {
        // do nothing.
    }
}
