/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

public class Timer {

	private long m_startTime;

	public Timer() {
		m_startTime = System.currentTimeMillis();
	}

	public void start() {

		m_startTime = System.currentTimeMillis();

	}

	public long elapsedTimeInMillis() {

		long currentTime = System.currentTimeMillis();

		return currentTime - m_startTime;
	}

}
