/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.db;

import org.springframework.context.ApplicationEvent;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractEventProcessor;

public class BaseDBWriter extends AbstractEventProcessor{

	@Override
	public void sendEvent(JetstreamEvent event) throws EventException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPendingEvents() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void shutDown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void pause() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void resume() {
		// TODO Auto-generated method stub
		
	}

}
