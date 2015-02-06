/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

public class EventLayout extends Layout {
	
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");

	@Override
	public void activateOptions() {
	}

	@Override
	public String format(LoggingEvent event) {
		return event.getRenderedMessage() + LINE_SEPARATOR;
	}

	@Override
	public boolean ignoresThrowable() {
		return false;
	}

}
