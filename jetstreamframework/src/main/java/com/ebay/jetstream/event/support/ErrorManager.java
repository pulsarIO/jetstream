/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.xmlser.XSerializable;

public class ErrorManager implements ErrorTracker, XSerializable {

	private Map<ManagedExceptionEntry, ManagedExceptionEntry> m_errors = new HashMap<ManagedExceptionEntry, ManagedExceptionEntry>();
	private int m_nErrorListMax = 25;
	private volatile String m_strLastToString;
	
	public void clearErrorList() {
		synchronized(m_errors) {
			m_errors.clear();
			m_strLastToString = null;
		}
	}
	
	@Override
	public String getErrors() {
		return toString();
	}
	
	@Override
	public String toString() {
		String strLocalString = m_strLastToString;
		if (strLocalString != null)
			return strLocalString;
		
		StringBuilder bldr = new StringBuilder();
		if (m_errors.size() > 0) {
			TreeSet<ManagedExceptionEntry> copy = new TreeSet<ManagedExceptionEntry>();
			synchronized(m_errors) {
				copy.addAll(m_errors.keySet());
			}
			for (ManagedExceptionEntry entry : copy)
				bldr.append(entry);
		}
		
		strLocalString = bldr.toString();
		m_strLastToString = strLocalString;
		return strLocalString;
	}
	
	public void registerError(Throwable t) {
		registerError(t, null);
	}
	
	public void registerError(Throwable t, JetstreamEvent evtCause) {
		if (m_errors.size() >= m_nErrorListMax)
			return;
		
		m_strLastToString = null;
		
		ManagedExceptionEntry entry = new ManagedExceptionEntry(t, evtCause);
		synchronized (m_errors) {
			ManagedExceptionEntry existing = m_errors.get(entry);
			if (existing != null)
				existing.incrementOccurranceCount();
			else 
				m_errors.put(entry, entry);
		}
	}
	
	public void setErrorListMax(int nSize) {
		m_nErrorListMax = nSize >= 0 ? Math.min(nSize, 50) : m_nErrorListMax;
	}
}
