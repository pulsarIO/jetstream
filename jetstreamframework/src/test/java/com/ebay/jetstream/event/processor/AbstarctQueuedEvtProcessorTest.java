/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ebay.jetstream.event.JetstreamEvent;

public class AbstarctQueuedEvtProcessorTest {

	@Test
	public void runTest(){
		JetstreamEvent msg=new JetstreamEvent();
		msg.put("helloTest", "Yes Test");
		
		TestProcessor testPr = new TestProcessor();
		testPr.sendEvent(msg);
		
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals(1, testPr.getTotalEventsReceived());
	}
}
