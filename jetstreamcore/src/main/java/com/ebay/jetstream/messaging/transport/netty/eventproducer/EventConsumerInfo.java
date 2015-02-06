/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import io.netty.channel.Channel;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.config.NettyTransportConfig;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.lb.Selection;
import com.ebay.jetstream.notification.AlertListener;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This class holds info about a given event consumer - advertisement, latest advisory IoSession, bound
 *          affinity keys. Sequence ids are doled out by this class for mesages destined to this consumer.
 * 
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DMI_RANDOM_USED_ONLY_ONCE")
public class EventConsumerInfo implements Selection, XSerializable, Externalizable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private EventConsumerAdvertisement m_advertisement;
	private Map<Channel, ConsumerChannelContext> m_channelContexts = new ConcurrentHashMap<Channel, ConsumerChannelContext>();
	private CopyOnWriteArrayList<Channel> m_channelIds = new CopyOnWriteArrayList<Channel>();
	private Object m_state = null;
	private Random m_random = new SecureRandom();
	private LinkedList<Object> m_affinitykeys = new LinkedList<Object>();
	private final Map<JetstreamTopic, AtomicLong> m_seqIdMap = new HashMap<JetstreamTopic, AtomicLong>();
	private String m_evConsumerInfoStr = "";
	private NettyVirtualQueueMonitor m_virtualQueueMonitor = new NettyVirtualQueueMonitor(3000);
	private Integer m_ndx = Integer.valueOf(0);
	private AlertListener m_alertListener;
	private NettyTransportConfig m_tcfg;
	final int HASH_SEED = 2147368987;
	HashFunction m_hf = Hashing.murmur3_128(HASH_SEED);
	
		
	@Hidden
	public NettyTransportConfig getTcfg() {
		return m_tcfg;
	}

	public void setTcfg(NettyTransportConfig tcfg) {
		this.m_tcfg = tcfg;
	}

		
	@Hidden
	public AlertListener getAlertListener() {
		return m_alertListener;
	}

	public void setAlertListener(AlertListener alertListener) {
		this.m_alertListener = alertListener;
	}

	public NettyVirtualQueueMonitor getVirtualQueueMonitor() {
		return m_virtualQueueMonitor;
	}

	public void setVirtualQueueMonitor(
			NettyVirtualQueueMonitor virtualQueueMonitor) {
		this.m_virtualQueueMonitor = virtualQueueMonitor;
		if (m_advertisement != null) {
			m_virtualQueueMonitor.setConsumerHost(m_advertisement.getHostName());
			m_virtualQueueMonitor.setAlertListener(m_tcfg.getAlertListener());
			m_virtualQueueMonitor.setMeasurementInterval(m_tcfg.getVqOFMeasurementInterval());
			m_virtualQueueMonitor.setOverflowCountThreshold(m_tcfg.getVqOverflowThreshold());
		}
	}

	public 	EventConsumerInfo() {
    
	}
	
	/**
	 * @param key
	 */
	public void bindAffinityKey(Object key) {
		if (!m_affinitykeys.contains(key)) {
			m_affinitykeys.add(key);
		}

	}

	/**
	 * 
	 */
	public void clearAllAffinityKeyBindings() {
		m_affinitykeys.clear();
	}

	/**
	 * @return
	 */
	public boolean containsMultipleAffinityKeyBindings() {
		return m_affinitykeys.size() > 1;
	}

		
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((m_advertisement == null) ? 0 : m_advertisement.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof EventConsumerInfo)) {
			return false;
		}
		EventConsumerInfo other = (EventConsumerInfo) obj;
		if (m_advertisement == null) {
			if (other.m_advertisement != null) {
				return false;
			}
		} else if (!m_advertisement.equals(other.m_advertisement)) {
			return false;
		}
		return true;
	}

	/**
	 * @return the advertisement
	 */
	public EventConsumerAdvertisement getAdvertisement() {
		return m_advertisement;
	}

		
	
	@Hidden
	public Object getSelectionState() {
		return m_state;
	}



	@Hidden
	public long getSeqid(JetstreamTopic topic) {

		if (m_seqIdMap.containsKey(topic)) {

			long seqid = m_seqIdMap.get(topic).getAndAdd(1);

			if (seqid < 0)
				m_seqIdMap.get(topic).set(0);
			return seqid;
		}
		else
			return 0;
	}

	/**
	 * @param topic
	 * @return
	 */
	public boolean interested(JetstreamTopic topic) {
		return m_advertisement.containsTopic(topic);
	}

	/**
	 * @param key
	 * @return
	 */
	public boolean isAffinityKeyBound(Object key) {
		return m_affinitykeys.contains(key);
	}

	
	
	

	
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		m_advertisement = (EventConsumerAdvertisement) in.readObject();
		m_affinitykeys = (LinkedList<Object>) in.readObject();
		m_virtualQueueMonitor = (NettyVirtualQueueMonitor) in.readObject();
		
	}

	/**
	 * @param advertisement
	 *          the advertisement to set
	 */
	public void setAdvertisement(EventConsumerAdvertisement advertisement) {
		m_advertisement = advertisement;

		if (m_advertisement != null) {
			Iterator<JetstreamTopic> itr = m_advertisement.getInterestedTopics().iterator();

			while (itr.hasNext()) {
				JetstreamTopic topic = itr.next();
				if (!m_seqIdMap.containsKey(topic)) {
					m_seqIdMap.put(topic, new AtomicLong(m_random.nextLong()));
				}
			}
			
			StringBuffer buf = new StringBuffer();
			buf.append(getHostAndPort());
			buf.append(m_advertisement.getConsumerId());
			
			m_evConsumerInfoStr = buf.toString();
			
		}
	}

	
	
	public void setSelectionState(Object state) {
		m_state = state;

	}

	

	/**
	 * @param key
	 */
	public void unbindAffinityKey(Object key) {
		if (!m_affinitykeys.contains(key)) {
			m_affinitykeys.remove(key);

		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	public void writeExternal(ObjectOutput out) throws IOException {

		out.writeObject(m_advertisement);
		out.writeObject(m_affinitykeys);
		out.writeObject(m_virtualQueueMonitor);
		
	}

	/**
	 * @return
	 */
	public String getHostAndPort() {

		return m_advertisement.getHostAndPort();

	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return m_evConsumerInfoStr;
	}
	
	
	
	/**
	 * @param channelId
	 * @param ctx
	 */
	public void setChannelContext(Channel channel, ConsumerChannelContext ctx) {
		
		if ((channel != null) && (channel.isActive()) && (ctx != null)) {
			
			Integer id = m_hf.hashString(ctx.getRemoteAddress()+ctx.getPort()).asInt(); // hack till Netty 4.0.18 is released - then replace with channel.id()
	
			m_channelContexts.put(channel, ctx);
			m_virtualQueueMonitor = ctx.getVirtualQueueMonitor();
			m_virtualQueueMonitor.setConsumerHost(m_advertisement.getHostName());
			m_virtualQueueMonitor.setAlertListener(m_tcfg.getAlertListener());
			m_virtualQueueMonitor.setMeasurementInterval(m_tcfg.getVqOFMeasurementInterval());
			m_virtualQueueMonitor.setOverflowCountThreshold(m_tcfg.getVqOverflowThreshold());
		
			if (!m_channelIds.contains(channel))
				m_channelIds.add(channel);
		}
	}
	
	
	
	/**
	 * @param channelId
	 * @return
	 */
	public ConsumerChannelContext getChannelContext(Channel channel) {
		return m_channelContexts.get(m_hf.hashString(channel.toString()).asInt()); // hack till netty 4.0.18 is released - then replace with channel.id() call
	}
	
	
	public ConsumerChannelContext getNextChannelContext() {
		
		if (m_channelIds.isEmpty()) {
			return null;
		}
		
		if (m_channelContexts.isEmpty()) {
			return null;
		}
		
		if (m_ndx >= m_channelIds.size())
			m_ndx = 0;
		
		Integer channelid = null;
		Channel channel = null;
		
		try {
			channel = m_channelIds.get(m_ndx);
		} catch (Throwable t) {
			return null;
		}
		
		m_ndx += 1;
		
		if (m_ndx >= m_channelIds.size())
			m_ndx = 0;
		
		return m_channelContexts.get(channel);
			
	}
	
		
	public Map<Channel, ConsumerChannelContext> getConsumerChannelContexts() {
		return Collections.unmodifiableMap(m_channelContexts);
	}
	
	
	
	public void markChannelAsDisconnected(Channel channel) {
		
		m_channelIds.remove(channel);
		
		m_channelContexts.remove(channel);
		

        
	}

	@Override
	public boolean isBusy() {
		return false;
	}
	
}
