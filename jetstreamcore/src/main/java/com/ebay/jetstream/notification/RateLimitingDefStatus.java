/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.notification;


import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.xmlser.XSerializable;


/***

 * 
 * @author gjin
 *
 */


/***
 * implement XSerializable so we can see the object display (for those gettable fields.
 *
 */
public class RateLimitingDefStatus  implements XSerializable{
	/* def */
	private long            m_windowSizeInMS;
	private int   	        m_maxNumOfAlerts;
	
	/* status for a window */
	private AtomicInteger   m_numOfAlertsProcessed = new AtomicInteger(0);
	private long            m_sinceTimeInMS;
	private AtomicInteger   m_dropCount = new AtomicInteger(0); /* total drop count for entire window */
	
	/*** whole life status of the application ***/
    private AtomicLong      m_totalAlertsDropped = new AtomicLong(0);
    private AtomicLong      m_totalAlertsSent = new AtomicLong(0);
    
	public static  RateLimitingDefStatus createUnRegisteredRDS() {
		RateLimitingDefStatus rds = new RateLimitingDefStatus();
		rds.m_windowSizeInMS = -1;
		rds.m_maxNumOfAlerts = -1;
		rds.m_sinceTimeInMS = System.currentTimeMillis();
		return rds;
	}
	
	public void setWindowSizeInMS(long size) {
		m_windowSizeInMS = size;
	}
	public long getWindowSizeInMS() {
		return m_windowSizeInMS;
	}
	public void setMaxNumOfAlerts(int max) {
		m_maxNumOfAlerts = max;
	}
	public int getMaxNumOfAlerts() {
		return m_maxNumOfAlerts;
	}
	public int getNumOfAlertsProcessed() {
		return m_numOfAlertsProcessed.get();
	}
	public long getDropCount() {
		return m_dropCount.get();
	}
	public Date getSinceDateTime(){
		return new Date(m_sinceTimeInMS);
	}
	
	public long getTotalAlertsSent() {
		return m_totalAlertsSent.get();
	}
	public long getTotalAlertsDropped(){
		return m_totalAlertsDropped.get();
	}
	
	public void initStatus() {
		m_numOfAlertsProcessed=new AtomicInteger(0);
		m_dropCount = new AtomicInteger(0);
		m_sinceTimeInMS = System.currentTimeMillis();
	}

	public void increaseDropCount() {
		m_dropCount.getAndIncrement();
		m_totalAlertsDropped.getAndIncrement();
	}
	public void increaseProcessedCount() {
		m_numOfAlertsProcessed.getAndIncrement();
		m_totalAlertsSent.getAndIncrement();
	}
	/*
	public void increaseTotalSentCount() {
		m_totalAlertsSent.getAndIncrement();
	}
	*/
	
	public static RateLimitingDefStatus cloneRateLimitiongDef(RateLimitingDefStatus old) {
		RateLimitingDefStatus rds = new RateLimitingDefStatus();
		rds.m_maxNumOfAlerts = old.m_maxNumOfAlerts;
		rds.m_windowSizeInMS = old.m_windowSizeInMS;
		return rds;
	}
	public boolean canSendOneMore() {
		long currentTimeInMS = System.currentTimeMillis();
		long timeDiffInMS = currentTimeInMS - m_sinceTimeInMS;
		
		if (timeDiffInMS < m_windowSizeInMS) {
			if (m_numOfAlertsProcessed.get() < m_maxNumOfAlerts) {
				return true;
			} else {
				return false;
			}
		} else {
			initStatus();
			return true;
		}
	}

}
