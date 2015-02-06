/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import kafka.common.QueueFullException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.AbstractOutboundChannel;
import com.ebay.jetstream.event.support.ErrorManager;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.util.RequestQueueProcessor;

/**
 * The outbound channel for kafka, it publish event to Kafka.
 * 
 * Use address to specify the kafka topics. Use serializer to convert event to
 * kafka message. The config is same as kafka producer config.
 * 
 * If specified multiple topics, it will send message to all topics one bye one.
 * 
 * @author xingwang
 */
@ManagedResource(objectName = "Event/Channel", description = "Kafka producer")
public class OutboundKafkaChannel extends AbstractOutboundChannel {
    private final class SendEventTask implements Runnable {
        private final JetstreamEvent theEvent;

        private SendEventTask(JetstreamEvent theEvent) {
            this.theEvent = theEvent;
        }

        @Override
        public void run() {
            realSendEvent(theEvent);
        }
    }

    private KafkaChannelAddress channelAddress;
    private KafkaMessageSerializer serializer;
    private KafkaProducerConfig m_config;
    private AtomicLong counter = new AtomicLong(0);
    protected final LongCounter dropByQueueFull = new LongCounter();
    protected final LongCounter dropBySerializationQueueFull = new LongCounter();
    protected final LongCounter dropByProducerException = new LongCounter();
    private List<Producer<byte[], byte[]>> m_producerList = new ArrayList<Producer<byte[], byte[]>>();
    private final AtomicInteger pendingCounter = new AtomicInteger(0);
    private final AtomicBoolean shutdownFlag = new AtomicBoolean(false);
    private volatile boolean disabled = false;
    private final ReadWriteLock lock = new ReentrantReadWriteLock(); 
    private final ErrorManager m_errors = new ErrorManager();

    private RequestQueueProcessor processor;
    
    private int serializationThreadPoolSize = 0;
    private int maxSerializationQueueSz = 262144;
    
	private static final Logger LOGGER = LoggerFactory.getLogger(OutboundKafkaChannel.class.getName());

    @Override
    public void afterPropertiesSet() throws Exception {
        if (serializationThreadPoolSize > 0) {
            processor = new RequestQueueProcessor(maxSerializationQueueSz, serializationThreadPoolSize, getBeanName() + "-Serialization");
        }
        Management.addBean(getBeanName(), this);
    }

    @ManagedOperation
	void clearErrorList() {
		m_errors.clearErrorList();
	}
    
    @Override
    public void close() {
        super.close();
        if (shutdownFlag.compareAndSet(false, true)) {
            closeProducers(m_producerList);
        }
    }

    private void closeProducers(List<Producer<byte[], byte[]>> producers) {
        Iterator<Producer<byte[], byte[]>> it = producers.iterator();
        while (it.hasNext()) {
            Producer<byte[], byte[]> producer = it.next();
            if (producer != null) {
                producer.close();
                producer = null;
            }
            it.remove();
        }
    }
    
    
    @Override
    public void flush() throws EventException {
        // do nothing
    }
    
    @ManagedAttribute
    public int getActiveProducerNum() {
        lock.readLock().lock();
        try {
            return m_producerList.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public KafkaChannelAddress getAddress() {
        return channelAddress;
    }

    public long getDropByProducerException() {
        return dropByProducerException.get();
    }

    public long getDropByQueueFull() {
        return dropByQueueFull.get();
    }

    public long getDropBySerializationQueueFull() {
        return dropBySerializationQueueFull.get();
    }
    
    public String getErrors() {
		return m_errors.toString();
	}

    public int getMaxSerializationQueueSz() {
        return maxSerializationQueueSz;
    }
    
    @Override
    public int getPendingEvents() {
        return pendingCounter.get();
    }

    @ManagedAttribute
    public KafkaProducerConfig getProducerConfig() {
        return m_config;
    }
    
    public long getSerializationQueueLength() {
        if (processor != null) {
            return processor.getPendingRequests();
        } else {
            return 0;
        }
    }

    public long getSerializationThreadFailureCount() {
        if (processor != null) {
            return processor.getDroppedRequests();
        } else {
            return 0;
        }
    }

    public int getSerializationThreadPoolSize() {
        return serializationThreadPoolSize;
    }

    private void init() {
    	m_producerList.clear();
        if (m_config.getEnabled()) {
            for (int i = 0, t = m_config.getPoolSize(); i < t; i++) {
                m_producerList.add(new Producer<byte[], byte[]>(new ProducerConfig(
                        m_config.getProps())));
            }
            disabled = false;
        } else {
            disabled = true;
        }
    }

    @Override
    public void open() {
        super.open();
        init();
    }

    @Override
    public void processApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextBeanChangedEvent) {

            ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

            // Check procuder config change
            ArrayList<Producer<byte[], byte[]>> oldProducers = new ArrayList<Producer<byte[], byte[]>>(m_producerList);
			if (bcInfo.isChangedBean(m_config)) {
                lock.writeLock().lock();
                try {
                    setConfig((KafkaProducerConfig) bcInfo.getChangedBean());
                    init();
                } finally {
                    lock.writeLock().unlock();
                    closeProducers(oldProducers);
                }
                
            } else if (bcInfo.isChangedBean(channelAddress)) {
                lock.writeLock().lock();
                try {
                    setAddress((KafkaChannelAddress) bcInfo.getChangedBean());
                    init();
                } finally {
                    lock.writeLock().unlock();
                    closeProducers(oldProducers);
                }
            }
        }
    }

    private void realSendEvent(JetstreamEvent event) throws EventException {
        pendingCounter.incrementAndGet();
        boolean injectTs = false;
        lock.readLock().lock();
        try {
            if (m_producerList.size() > 0) {
                byte[] key = null;
                byte[] m = null;
                boolean serialized = false; 
                
                String[] forwardingTopics = event.getForwardingTopics();
                
                for (String topic : channelAddress.getChannelTopics()) {
                    if (forwardingTopics != null) {
                        boolean bFound = false;
                        for (String strTopic : forwardingTopics) {
                            if (strTopic.equals(topic)) {
                                bFound = true;
                                break;
                            }
                        }
                        if (!bFound)
                            continue;
                    }
                    if (!serialized) {
                        if (m_config.getTimestampKey() != null && !event.containsKey(m_config.getTimestampKey())) {
                            event.put(m_config.getTimestampKey(), System.currentTimeMillis());
                            injectTs = true;
                        }
                        
                        key = serializer.encodeKey(event);
                        m = serializer.encodeMessage(event);
                        
                        serialized = true;
                    }
                    KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(
                            topic, key, m);
                    scheduleNextProducer(message);
                }
            } else {
                incrementEventDroppedCounter();
            }
        } finally {
            pendingCounter.decrementAndGet();
            lock.readLock().unlock();
            if (injectTs) {
                event.remove(m_config.getTimestampKey());
            }
            setLastEvent(event);
        }
    }
    
    private void scheduleNextProducer(KeyedMessage<byte[], byte[]> message) {
        int producerIndex = (int) (counter.getAndIncrement() % m_producerList.size());
        Producer<byte[], byte[]> producer = m_producerList.get(producerIndex);
        try {
            producer.send(message);
            incrementEventSentCounter();
        } catch (QueueFullException ex) {
            incrementEventDroppedCounter();
            dropByQueueFull.increment();
        } catch (Throwable ex) {
            incrementEventDroppedCounter();
            m_errors.registerError(ex);
            dropByProducerException.increment();
        }
    }
    
    @Override
    public void sendEvent(JetstreamEvent event) throws EventException {
        if (disabled) {
            return;
        }
        incrementEventRecievedCounter();
        if (shutdownFlag.get()) {
            incrementEventDroppedCounter();
            return;
        }
        
        if (processor == null) {
            realSendEvent(event);
        } else {
            boolean success = processor.processRequest(new SendEventTask(event));
            if (!success) {
                dropBySerializationQueueFull.increment();
                incrementEventDroppedCounter();
            }
        }
    }
    

    public void setAddress(KafkaChannelAddress channelAddress) {
        this.channelAddress = channelAddress;
    }
    
    public void setConfig(KafkaProducerConfig config) {
        this.m_config = config;
    }

    public void setMaxSerializationQueueSz(int maxWorkQueueSz) {
        this.maxSerializationQueueSz = maxWorkQueueSz;
    }
    
    public void setSerializationThreadPoolSize(int threadPoolSize) {
        this.serializationThreadPoolSize = threadPoolSize;
    }

	public void setSerializer(KafkaMessageSerializer serializer) {
        this.serializer = serializer;
    }

	@Override
    public void shutDown() {
        if (shutdownFlag.compareAndSet(false, true)) {
            if (processor != null) {
                processor.shutdown();
            }
            closeProducers(m_producerList);
        }
        
        LOGGER.warn("final events sent = " + getTotalEventsSent() +
                "final total events dropped =" + getTotalEventsDropped() +
                "final total events dropped by producer exception =" + getDropByProducerException() +
                "final total events dropped by queue full =" + getDropByQueueFull() +
                "final total events dropped by serialization queue full =" + getDropBySerializationQueueFull() +
                "final total events received =" + getTotalEventsReceived()); 
                
    }

}

