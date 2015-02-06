/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support;

import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.event.JetstreamEvent;

public interface ErrorTracker {

	@ManagedOperation
	void clearErrorList();
	String getErrors() ;
	void setErrorListMax(int nSize);
	void registerError(Throwable t);
	void registerError(Throwable t, JetstreamEvent evtCause);
}
