/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support.channel;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.channel.InboundChannel;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.messagetype.MapMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.util.CommonUtils;
/**
 * This bean was used to receive the replayNotification topic and
 * control the inbound replay channel.
 * 
 * It should be injected to the ChannelBinder. 
 * 
 * The bean must depend on the MessageService. 
 * 
 * @author xingwang
 *
 */
@ManagedResource(objectName = "Event/RemoteController", description = "Remote channel controller")
public class RemoteController extends AbstractNamedBean 
    implements IMessageListener, InitializingBean, BeanNameAware, DisposableBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteController.class.getPackage().getName());
    public static final String KEY_SERVER = "js_advice_server";
    public static final String KEY_COMMAND = "js_advice_command";
    
    public static final String COMMAND_START = "startReplay";
    public static final String COMMAND_STOP = "stopReplay";
    private static final int HISTORY_COUNT = 32;
    
    private List<String> m_cmdHistory = new ArrayList<String>(HISTORY_COUNT);
    private AtomicLong m_historySeq = new AtomicLong(0);
    
    private String m_replayNotificationTopic;
    private InboundChannel m_inboundChannel;
    
    private ScheduledExecutorService m_watchDog;

    private int m_maxPausedTimeInMs = 60 * 60 * 1000; //one hour
    private Map<String, Long> m_stateMap = new HashMap<String, Long>(); // { server -> pausedTime }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        Management.addBean(getBeanName(), this);
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
    
    private void checkStateMap() {
        synchronized (this) {
            if (m_stateMap.isEmpty()) {
                return;
            }
            
            long currentTimeMillis = System.currentTimeMillis();
                
            Iterator<Map.Entry<String, Long>> iter = m_stateMap.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<String, Long> entry = iter.next();
                if (entry.getValue() + m_maxPausedTimeInMs < currentTimeMillis) {
                    addToHistory("Timeout " + entry.getKey());
                    iter.remove();
                }
            }
            
            if (m_stateMap.isEmpty()) {
                addToHistory("Resume by watch dog");
                m_inboundChannel.resume();
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        if (m_watchDog != null) {
            m_watchDog.shutdownNow();
        }
    }

    @ManagedAttribute
    public int getMaxPausedTimeInMs() {
        return m_maxPausedTimeInMs;
    }
    
    public String getReplayNotificationTopic() {
        return m_replayNotificationTopic;
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
    
    
    @ManagedAttribute
    public Map<String, Long> getStateMap() {
        return m_stateMap;
    }

    private void handleMessage(JetstreamMessage m) {
        synchronized (this) {
            MapMessage msg = (MapMessage) m;
            String command = (String) msg.get(KEY_COMMAND);
            String server = (String) msg.get(KEY_SERVER);
            
            addToHistory("Receive " + command + " from " + server);
            
            if (COMMAND_STOP.equals(command)) {
                if (m_stateMap.isEmpty()) {
                    addToHistory("Pause");
                    m_inboundChannel.pause();
                }
                m_stateMap.put(server, System.currentTimeMillis());
            } else if (COMMAND_START.equals(command)) {
                if (m_stateMap.remove(server) != null && m_stateMap.isEmpty()) {
                    addToHistory("Resume");
                    m_inboundChannel.resume();
                }
            }
        }
    }

    @Override
    public void onMessage(JetstreamMessage m) {
        try {
            handleMessage(m);
        } catch (Exception ex) {
            LOGGER.error( "Fail to handle replay message.", ex);
        }
    }

    void setInboundChannel(InboundChannel inboundChannel) {
        this.m_inboundChannel = inboundChannel;
    }

    /**
     * Max paused time in ms. The channel will be auto resumed when
     * it exceeds max paused time.
     * 
     * @param maxPausedTimeInMs
     */
    public void setMaxPausedTimeInMs(int maxPausedTimeInMs) {
        this.m_maxPausedTimeInMs = maxPausedTimeInMs;
    }
    
    public void setReplayNotificationTopic(String topic) {
        this.m_replayNotificationTopic = topic;
    }

    void subscribe() {
        MessageService ms = MessageService.getInstance();

        if (ms.isInitialized()) {
            try {
                ms.subscribe(new JetstreamTopic(m_replayNotificationTopic), this);

                m_watchDog = Executors.newScheduledThreadPool(1);
                m_watchDog.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            checkStateMap();
                        } catch (Exception ex) {
                            LOGGER.error( "Fail to run watch dog task.", ex);
                        }
                    }
                    
                }, 1, 1, TimeUnit.MINUTES);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info( "Subscribed for Mongo Config Change Information using Message Service");
                }

            } catch (Exception e) {
                throw CommonUtils.runtimeException(e);
            }
        } else {
            LOGGER.error( "Message Service not initialized - unable to register listener");
        }
    }
}
