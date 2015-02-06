/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import java.util.TimerTask;

/**
 * @author shmurthy
 *
 * 
 */


public class DispatchQueueMonitor extends TimerTask {

	private final static int ONCE_EVERY_FIVE_SECS = 5000;
	private MessageServiceTimer m_timer = MessageServiceTimer.sInstance();

	
	/* (non-Javadoc)
	 * @see java.util.TimerTask#run()
	 */
	@Override
	public void run() {
		
		MessageService.getInstance().postDispatchQueueStats();
		
	}

	/**
	 * 
	 */
	public void shutdown()
	{
		cancel();
	}
	
	/**
	 * 
	 */
	public void start()
	{
		m_timer.schedulePeriodicTask(this, DispatchQueueMonitor.ONCE_EVERY_FIVE_SECS);

	}
}
