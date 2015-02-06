/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.batcher;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.Test;

public class AutoFlushBatcherTest {

	public static class MyBatcher<T> extends AutoFlushBatcher<T> {

		AtomicInteger count = new AtomicInteger(0);

		@Override
		public void flush(List<T> items) {

			Iterator<T> iter = items.iterator();

			while(iter.hasNext()) {
				System.out.println(iter.next());
				count.addAndGet(1);

			}

		}

		int getCount()
		{
			return count.get();
		}

	}

	public AutoFlushBatcherTest() {}

	@Test
	public void testBatcher() {
		MyBatcher<Integer> b = new MyBatcher<Integer>();

		for (int i=0; i<100; i++) {
			try {
				Integer e = new Integer(i);

				b.write(e);
			} catch (Exception e) {
				System.out.println("Caught Exception - " + e.getLocalizedMessage());
				Assert.fail();
			}
		}

		int j=0;

		while(true) {
			try {
				if (++j > 2) {
					b.shutdown();
					break;
				}

				Thread.sleep(500);
			} catch (InterruptedException e) {
				System.out.println("Caught Exception - " + e.getLocalizedMessage());
				Assert.fail();
			}
		}

		System.out.println(b.getCount());

		Assert.assertEquals(b.getCount(), 100);

	}
}