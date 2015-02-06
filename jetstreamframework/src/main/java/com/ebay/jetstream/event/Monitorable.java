/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;


public interface Monitorable {
	long getTotalEventsReceived();
	long getTotalEventsSent();
	long getTotalEventsDropped();
	long getEventsReceivedPerSec();
	long getEventsSentPerSec();
	long getTotalPauseCount();
	long getTotalResumeCount();
		
}
