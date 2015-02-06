/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventscheduler;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventConsumerInfo;
import com.ebay.jetstream.messaging.transport.netty.registry.Registry;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * 
 * Abstraction for implementing a scheduler.
 * 
 */
public interface Scheduler extends Cloneable {
	
	/**
	 * @return true if the scheduler supports affinity
	 */
	public boolean supportsAffinity();
	
	/**
	 * @param info
	 */
	public void addEventConsumer(EventConsumerInfo info);
	
	/**
	 * @param info
	 */
	public void removeEventConsumer(EventConsumerInfo info);
	
	
	/**
	 * @param msg
	 * @param registry
	 * @return
	 * @throws NoConsumerToScheduleException
	 */
	public EventConsumerInfo scheduleNext(JetstreamMessage msg, Registry registry) throws NoConsumerToScheduleException;
	
	
	/**
	 * 
	 */
	public void shutdown();
	
	/**
	 * @return
	 * @throws CloneNotSupportedException
	 */
	public  Scheduler clone() throws CloneNotSupportedException;
}
