/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.stats;

import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author shmurthy
 *
 * 
 */
@ManagedResource
public class MessageServiceStatsController implements XSerializable {

	private MessageService m_msvc;

	/**
	 * 
	 */
	public MessageServiceStatsController() {}

	/**
	 * @param svc
	 */
	public MessageServiceStatsController(MessageService svc)
	{
		m_msvc = svc;
	}

	/**
	 * @return
	 */
	public MessageServiceStats getStats()
	{

		return m_msvc.getStats();
	}

	/**
	 * 
	 */
	@ManagedOperation
	public void resetStats()
	{
		m_msvc.resetStats();
	}
}