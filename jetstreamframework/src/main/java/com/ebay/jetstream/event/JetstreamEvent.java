/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NamedBean;

import com.ebay.jetstream.util.CommonUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * JetstreamEvent is a generalized event structure for wrapping underlying event implementations. The JetstreamEvent type is
 * used throughout the event framework for processing events. There are few distinguished attributes of the event,
 * primarily required for routing and state management. The remaining contents of the events are opaque, with the
 * semantics being defined by the query processing statements. <br>
 * <br>
 * The attributes of the events are retrieved via the Map.get method. The attributes specified by EQL are keys directly
 * into the Map.get method. The key must be a string while the type of the value can vary depending upon the context.
 * EQL will attempt to make the appropriate type conversions.
 * 
 * Metadata can be passed in this event between various stages within the process. The metadata will not be serialized over the wire.
 * 
 * @author shmurthy@ebay.com - derived from original impl created by Dan Pritchett
 * 
 */


public class JetstreamEvent implements Map<String, Object>, Externalizable,  Cloneable, KryoSerializable {

    private static final long serialVersionUID = -2339580269438201272L;
    
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event");
	private String[] m_topics;
	private String[] m_urls;
	private final Map<String, Object> m_backingMap;
	private final Map<String, Object> m_metadata = new HashMap<String, Object>(2);

	public JetstreamEvent() {
		this(new HashMap<String, Object>());
	}

	public JetstreamEvent(Map<String, Object> map) {
		this(null, null, map);
		 
		
	}

	public JetstreamEvent(String eventType, EventId id, Map<String, Object> map) {
		m_backingMap = map != null ? map : new HashMap<String, Object>();
		if (eventType != null)
			setEventType(eventType);
		if (id != null)
			setEventId(id);
	}
	
	public void addMetaData(String key, Object value) {
		m_metadata.put(key, value);
	}
	
	public Object getMetaData(String key) {
		return m_metadata.get(key);
	}

	@Override
	public void clear() {
		m_backingMap.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return m_backingMap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return m_backingMap.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return m_backingMap.entrySet();
	}

	@Override
	public boolean equals(Object o) {
		return super.equals(o) || m_backingMap.equals(o);
	}

	@Override
	public Object get(Object key) {
		return m_backingMap.get(key);
	}

	/**
	 * The event id is an opaque handle to the event. This is used for state tracking in the ESP system.
	 * 
	 * @return the opaque event identifier
	 */
	public EventId getEventId() {
		return (EventId) get(JetstreamReservedKeys.EventId.toString());
	}

	/**
	 * All events must be defined by a type. The ESP system considers an event to be uniquely identified by the
	 * combination of type and identifier. Types are also used in some cases for routing events.
	 */
	public String getEventType() {
		return (String) get(JetstreamReservedKeys.EventType.toString());
	}

	/**
	 * @return A copy of this event with any entries held with a reserved key stripped
	 */
	public JetstreamEvent getFilteredEvent() {
		JetstreamEvent newEvent = new JetstreamEvent();
		for (Map.Entry<String, Object> entry : m_backingMap.entrySet()) {
			if (JetstreamReservedKeys.isReserved(entry.getKey()))
				continue;
			newEvent.put(entry.getKey(), entry.getValue());
		}
		return newEvent;
	}

	public String[] getForwardingTopics() {
		return m_topics;
	}

	public String[] getForwardingUrls() {
		return m_urls;
	}

	@Override
	public int hashCode() {
		return m_backingMap.hashCode();
	}
	
	@Override
	public boolean isEmpty() {
		return m_backingMap.isEmpty();
	}

	@Override
	public Set<String> keySet() {
		return m_backingMap.keySet();
	}

	/**
	 * Any caller is required to pass a source Identifier which will be part of the log message. The 
	 * implementation of this method must add the event ID to the log message along with the passed source Identifier
	 * 
	 * @param source
	 * @return
	 */
	public void log(NamedBean source) {
		LOGGER.debug( "eventSource=" + source.getBeanName() + "&eventId = " + getEventId());
	}
	

	@Override
	public Object put(String key, Object value) {
		return m_backingMap.put(key, value);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> t) {
		m_backingMap.putAll(t);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		int mapSize = in.readInt();
		for (int i = 0; i < mapSize; i++)
			m_backingMap.put((String)in.readObject(), (Serializable) in.readObject());
	}

	@Override
	public Object remove(Object key) {
		return m_backingMap.remove(key);
	}

	public void setEventId(EventId id) {
		put(JetstreamReservedKeys.EventId.toString(), id);
	}

	public void setEventType(String type) {
		put(JetstreamReservedKeys.EventType.toString(), type);
	}

	public void setForwardingTopics(String[] topics) {
		m_topics = topics;
	}

	public void setForwardingUrls(String[] urls) {
		m_urls = urls;
	}

	@Override
	public int size() {
		return m_backingMap.size();
	}

	@Override
	public String toString() {
		return m_backingMap.toString();
	}

	@Override
	public Collection<Object> values() {
		return m_backingMap.values();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
				
		out.writeInt(m_backingMap.size());
		Iterator<Entry<String, Object>> itr = m_backingMap.entrySet().iterator();

		while (itr.hasNext()) {

			Map.Entry<String, Object> entry1 = itr.next();
			
			out.writeObject(entry1.getKey());
			out.writeObject(entry1.getValue());

		}

	}
	
	public JetstreamEvent clone() throws CloneNotSupportedException  {
		
		JetstreamEvent event;
		
		try {
			event = CommonUtils.getDeepCopy(this);
		} catch (InstantiationException e) {
			return null;
		} catch (IllegalAccessException e) {
			return null;
		}
		
		
		event.m_metadata.putAll(this.m_metadata);
		
		if (this.m_urls != null) {
			event.m_urls = new String[this.m_urls.length];
			System.arraycopy(this.m_urls, 0, event.m_urls, 0, this.m_urls.length);
		}
		
		if (this.m_topics != null) {
			event.m_topics = new String[this.m_topics.length];
			System.arraycopy(this.m_topics, 0, event.m_topics, 0, this.m_topics.length);
		}
		
		return event;
	}

	@Override
	public void write(Kryo kryo, Output out) {
		
		out.writeInt(m_backingMap.size(), true);
		Iterator<Entry<String, Object>> itr = m_backingMap.entrySet().iterator();

		while (itr.hasNext()) {

			Map.Entry<String, Object> entry1 = itr.next();
			out.writeString(entry1.getKey());
			kryo.writeClassAndObject(out, entry1.getValue());
		}
	}

	@Override
	public void read(Kryo kryo, Input in) {
		int mapSize = in.readInt(true);
		for (int i = 0; i < mapSize; i++) {
			m_backingMap.put(in.readString(), kryo.readClassAndObject(in));
		}
	}
	
}
