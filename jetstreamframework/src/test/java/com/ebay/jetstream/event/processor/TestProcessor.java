/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.MonitorableStatCollector;

public class TestProcessor extends AbstractQueuedEventProcessor{

	@Override
	public int getPendingEvents() {
		return 0;
	}

	@Override
	protected String getComponentName() {
		return "Test Processor";
	}

	@Override
	protected EventProcessRequest getProcessEventRequest(JetstreamEvent event) {
		EventProcessRequest req = new TestEventRequest(event, this);
		return req;
	}

	@Override
	protected void init() {
		// TODO Auto-generated method stub
	}
	
	
	class TestEventRequest extends EventProcessRequest {

		public TestEventRequest(JetstreamEvent event,
				MonitorableStatCollector parent) {
			super(event, parent);
		}

		@Override
		protected void processEvent(JetstreamEvent event) throws Exception {
			System.out.println("event received");
			
			
		}
		
	}

}
