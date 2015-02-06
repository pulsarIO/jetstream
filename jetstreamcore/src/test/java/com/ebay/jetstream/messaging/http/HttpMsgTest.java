/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.http;

import java.util.concurrent.atomic.AtomicBoolean;

public interface HttpMsgTest {
	
	public AtomicBoolean getTestPassed() ;

	public void setTestPassed(AtomicBoolean m_testPassed);

}
