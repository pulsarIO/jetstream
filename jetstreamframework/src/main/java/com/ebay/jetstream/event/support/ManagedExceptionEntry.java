/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicInteger;

import com.ebay.jetstream.event.JetstreamEvent;

final class ManagedExceptionEntry implements Comparable<ManagedExceptionEntry> {

	private final String m_strEvent;
	private final String m_strStackTrace;
	private final AtomicInteger m_occurrance = new AtomicInteger(1);
	
	ManagedExceptionEntry(Throwable t, JetstreamEvent evTriggeringEvent) {
		Writer wtr = new StringWriter();
		PrintWriter pw = new PrintWriter(wtr);
		t.printStackTrace(pw);
		pw.flush();
		m_strStackTrace = wtr.toString();
		m_strEvent = (evTriggeringEvent != null) ? evTriggeringEvent.toString() : "{\"triggerevent\":\"unknown\"}";
	}
	
	void incrementOccurranceCount() {
		m_occurrance.incrementAndGet();
	}
	
	@Override
	public int hashCode() {
		return m_strStackTrace.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		return (obj instanceof ManagedExceptionEntry) && ((ManagedExceptionEntry)obj).m_strStackTrace.equals(m_strStackTrace);
	}

	@Override
	public int compareTo(ManagedExceptionEntry o) {
		int nComp = (o != null) ? m_occurrance.get() - m_occurrance.get() : 1;
		if (nComp == 0)
			nComp = o.m_strStackTrace.compareTo(m_strStackTrace);
		return nComp;
	}
	
	@Override
	public String toString() {
		StringBuilder bldr = new StringBuilder();
		bldr.append("Causing Event: ").append(m_strEvent).append("\n");
		bldr.append("Occurance Count: ").append(m_occurrance).append("\n");
		bldr.append("Stack Trace: ").append(m_strStackTrace).append("\n");
		bldr.append("------------------------------------------------\n");
		return bldr.toString();
	}
}
