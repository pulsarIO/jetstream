/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.junit.Test;

public class NameableThreadFactoryTest {
	private static final String THREAD_NAME = "TestThreadFactory";

	private final CountDownLatch endGate = new CountDownLatch(1);
	
	@Test
	public void testNameableThreadFactory() {
		ThreadFactory tf = new NameableThreadFactory(null);
		
		Thread thread = tf.newThread(null);
		assertEquals("null-1", thread.getName());
		
		tf = new NameableThreadFactory(THREAD_NAME);
		thread = tf.newThread(null);
		assertEquals(THREAD_NAME + "-1", thread.getName());
		assertEquals(Thread.NORM_PRIORITY, thread.getPriority());
		assertFalse(thread.isDaemon());
		assertEquals(Thread.currentThread().getThreadGroup(), thread.getThreadGroup());
	}

	@Test
	public void testRunThread() {
		ExecutorService executor = Executors.newSingleThreadExecutor(new NameableThreadFactory(THREAD_NAME));
		executor.execute(new ThreadFactoryRunner());
		
		try {
	        endGate.await();
        } catch (InterruptedException e) {
	        e.printStackTrace();
        }
		
		executor.shutdown();
	}
	
	class ThreadFactoryRunner implements Runnable {

		@Override
        public void run() {
			Thread thread = Thread.currentThread();
			System.out.println(thread.getName());
			assertEquals(THREAD_NAME + "-1", thread.getName());
			assertEquals(Thread.NORM_PRIORITY, thread.getPriority());
			assertFalse(thread.isDaemon());
			
			endGate.countDown();
        }
		
	}
}
