/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;


import java.util.Timer;
import java.util.TimerTask;

/**
 * @author shmurthy
 *
 * Timer used by MessageService
 */

public class MessageServiceTimer {

	private static MessageServiceTimer m_sTheInstance = new MessageServiceTimer();
	private Timer m_timer = new Timer();
	private boolean m_timerCanceled = false;
	
	
	/**
	 * 
	 */
	private MessageServiceTimer() {
			
	}
	
	/**
	 * @return
	 */
	public static MessageServiceTimer sInstance()
	{
		
		return m_sTheInstance;
	}
	
	/**
	 * @return
	 */
	public synchronized Timer getTimer()
	{
		if (m_timerCanceled) {
			m_timer = new Timer();
			m_timerCanceled = false;
		}
		
		return m_timer;
	}
	
	/**
	 * 
	 */
	public synchronized void shutdown()
	{
		if (m_timer != null)
			m_timer.cancel();
		
		m_timerCanceled = true;
		
	}

	/**
     * schedule a task that starts immediately
	 * @param task
	 * @param period
	 */
	public synchronized void schedulePeriodicTask(TimerTask task, long period)
	{
		try {
			m_timer.scheduleAtFixedRate(task, 0, period);
		} catch (IllegalStateException ie) {
			m_timer = new Timer();
			m_timer.scheduleAtFixedRate(task, 0, period);
		}
	}
	
    
    /**
     * @param task
     * @param period
     */
    public synchronized void schedulePeriodicTask(TimerTask task, long period, long delay)
    {
    	try {
    		m_timer.scheduleAtFixedRate(task, delay, period);
    	} catch (IllegalStateException ie) {
			m_timer = new Timer();
			m_timer.scheduleAtFixedRate(task, delay, period);
		}
    }
    
	/**
	 * @param task
	 * @param delay
	 */
	public synchronized void scheduleOneTimeTask(TimerTask task, long delay)
	{
		try {
			m_timer.schedule(task, delay);
		} catch (IllegalStateException ie) {
			m_timer = new Timer();
			m_timer.schedule(task, delay);
		}
	}
	
}
