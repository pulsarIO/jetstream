/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.NamedBean;

import com.ebay.jetstream.xmlser.XSerializable;

/**
 * dynamic sinks are added to rewire the pipeline dynamically using dynamic config
 *
 */
public class EventSinkList extends CopyOnWriteArrayList<EventSink> implements XSerializable,NamedBean,BeanNameAware {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		EventSinkList other = (EventSinkList) obj;
		if (m_name == null) {
			if (other.m_name != null)
				return false;
		} else if (!m_name.equals(other.m_name))
			return false;
		return true;
	}

	public EventSinkList() {}
	
	public EventSinkList(Collection<EventSink> sinks) {
		super(sinks);
	}
	
	public Collection<EventSink> getSinks() {
		return Collections.unmodifiableList(this);
	}
	
	public void setSinks(Collection<EventSink> sinks) {
		addAll(sinks);
	}

	public void addSink(EventSink sink) {
		(this).add(sink);
	}
	
	public void removeSink(EventSink sink) {
		(this).remove(sink);
	}

	private String m_name;

	public String getBeanName() {
		return m_name;
	}

 
	public void setBeanName(String name) {
		m_name = name;
	}

	@Override
	public String toString() {
		return getBeanName();
	}

}
