/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.MonitorableStatCollector;

public class EventRequestTest extends EventProcessRequest{

	protected EventRequestTest(JetstreamEvent event,
			MonitorableStatCollector parent) {
		super(event, parent);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void processEvent(JetstreamEvent event) throws Exception {
		
	}

}
