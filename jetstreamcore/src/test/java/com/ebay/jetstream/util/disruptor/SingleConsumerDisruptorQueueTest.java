/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.disruptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import junit.framework.Assert;

import org.junit.Test;

public class SingleConsumerDisruptorQueueTest {
	@Test
	public void testQueue() throws Exception {
	    for (int i = 1; i < 5; i++) {
	        System.out.println("Producers :" + i);
    		testQueue(new SingleConsumerDisruptorQueue<String>(65536, false), i);
	    }

	}

    private void testQueue(final BlockingQueue<String> queue, int producerNum) throws InterruptedException {
        final long count = 1<<24; //32M
        final long cc = count * producerNum;

		Runnable consumer = new Runnable() {

			@Override
			public void run() {
				int c = 0;
				while (c < cc) {
					try {
						queue.take();
					} catch (Throwable e) {
						e.printStackTrace();
					}
					c++;
				}
			}
		};
		
		List<Runnable> producers = new ArrayList<Runnable>();
		List<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < producerNum; i++) {
    		Runnable producer = new Runnable() {
    
    			@Override
    			public void run() {
    				int c = 0;
    				while (c < count) {
    					try {
    					queue.put("x");
    					}
    					catch (Throwable ex) {
    						ex.printStackTrace();
    					}
    					c++;
    				}
    			}
    		};
    		producers.add(producer);
    		threads.add(new Thread(producer));
		}
		
		Thread t2 = new Thread(consumer);
		long begin = System.currentTimeMillis();
		t2.start();
		for (Thread t : threads) {
		    t.start();
		}
		t2.join();
		long end = System.currentTimeMillis();
        for (Thread t : threads) {
            t.join();
        }
        Assert.assertEquals(queue.size(), 0);
        Assert.assertTrue(queue.isEmpty());
		System.out.println(queue.getClass().getSimpleName() + " transfer rate : " + (cc / (end - begin)) + " per ms, Used " +  (end - begin) + "ms for " + cc);
    }
	
	@Test
	public void testBlockingQueue() {
	    int capacity = 8192;
	    int count = 10000;
	    SingleConsumerDisruptorQueue<String> queue = new SingleConsumerDisruptorQueue<String>(8192, false);
	        
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(queue.size(), Math.min(i, capacity));
            queue.offer("" + i);
        }
        Assert.assertEquals(queue.size(), capacity);
        for (int i = 0; i < capacity; i++) {
            Assert.assertEquals(queue.size(), capacity - i);
            Assert.assertEquals(queue.peek(), "" + i);
            Assert.assertEquals(queue.poll(), "" + i);
        }
        Assert.assertEquals(queue.size(), 0);
	}
}
