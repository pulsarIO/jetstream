/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.console;



import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.support.AbstractEventProcessor;
import com.ebay.jetstream.management.Management;

@ManagedResource(objectName = "Event/Processor", description = "Console Writer")
public class ConsoleLoggingProcessor extends AbstractEventProcessor {

	@SuppressWarnings("restriction")
	public void afterPropertiesSet() throws Exception {
		Management.addBean(getBeanName(), this);
	}

	public void sendEvent(JetstreamEvent event) throws EventException {
		System.out.println(event);
	}

	@Override
	public String toString() {
		return getBeanName();
	}

	@Override
	public void pause() {
		// TODO Auto-generated method stub
	}

	@Override
	public void resume() {
		// TODO Auto-generated method stub
	}

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		// TODO Auto-generated method stub
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher arg0) {
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

}
