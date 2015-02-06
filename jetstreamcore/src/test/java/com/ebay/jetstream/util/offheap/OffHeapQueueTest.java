/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.util.offheap.QueueBuilder.Unit;
import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;

public class OffHeapQueueTest {
	
	@Test
	public void testProducerConsumer() throws Exception {
	    QueueBuilder<String> builder = QueueBuilder.newBuilder();
	    builder.withCapacity(6000).withSerializer(DefaultSerializerFactory.getInstance().getStringSerializer())
	        .withMaxMemory(128).withUnit(Unit.MB);
	    final BlockingQueue<String> queue = builder.buildBlockingQueue();
        
		final long count = 1<<10;
		Runnable consumer = new Runnable() {

			@Override
			public void run() {
				int c = 0;
				while (c < count) {
					try {
						queue.take();
					} catch (Throwable e) {
						e.printStackTrace();
					}
					c++;
				}
			}
		};
		
		Runnable producer = new Runnable() {

			@Override
			public void run() {
				int c = 0;
				while (c < count) {
					try {
					queue.put("" + System.currentTimeMillis());
					}
					catch (Throwable ex) {
						ex.printStackTrace();
					}
					c++;
				}
			}
		};
		
		Thread t1 = new Thread(producer);
		Thread t2 = new Thread(consumer);
		Thread t3 = new Thread(producer);
		Thread t4 = new Thread(consumer);
		t2.start();
		t1.start();
		t3.start();
		t4.start();
		t1.join(30000);
		t2.join(1000);
		t3.join(1000);
		t4.join(1000);
	}
	
    @Test
    public void testUnboundedBlockingQueue() {
        QueueBuilder<String> builder = QueueBuilder.newBuilder();
        builder.withCapacity(Integer.MAX_VALUE).withSerializer(DefaultSerializerFactory.getInstance().getStringSerializer())
                .withMaxMemory(10).withUnit(Unit.MB);
        final BlockingQueue<String> queue = builder.buildBlockingQueue();

        int count = 0;
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (!queue.offer("" + i)) {
                break;
            }
            count = i;
        }
        Assert.assertEquals(queue.size(), count + 1);
        for (int i = 0; i < count + 1; i++) {
            Assert.assertEquals(queue.peek(), "" + i);
            Assert.assertEquals(queue.poll(), "" + i);
        }
        Assert.assertEquals(queue.size(), 0);
    }

    public void testUnboundedQueue() {
        QueueBuilder<String> builder = QueueBuilder.newBuilder();
        builder.withCapacity(Integer.MAX_VALUE).withSerializer(DefaultSerializerFactory.getInstance().getStringSerializer())
                .withMaxMemory(10).withUnit(Unit.MB);
        final Queue<String> queue = builder.buildQueue();

        int count = 0;
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (!queue.offer("" + i)) {
                break;
            }
            count = i;
        }
        Assert.assertEquals(queue.size(), count + 1);
        for (int i = 0; i < count + 1; i++) {
            Assert.assertEquals(queue.peek(), "" + i);
            Assert.assertEquals(queue.poll(), "" + i);
        }
        Assert.assertEquals(queue.size(), 0);
    }
    
	@Test
	public void testBlockingQueue() {
	    int capacity = 5000;
	    int count = 10000;
        QueueBuilder<String> builder = QueueBuilder.newBuilder();
        builder.withCapacity(capacity).withSerializer(DefaultSerializerFactory.getInstance().getStringSerializer())
                .withMaxMemory(128).withUnit(Unit.MB);
        final BlockingQueue<String> queue = builder.buildBlockingQueue();
	        
		
        for (int i = 0; i < count; i++) {
            queue.offer("" + i);
        }
        Assert.assertEquals(queue.size(), capacity);
        for (int i = 0; i < capacity; i++) {
            Assert.assertEquals(queue.peek(), "" + i);
            Assert.assertEquals(queue.poll(), "" + i);
        }
        Assert.assertEquals(queue.size(), 0);
	}
	
	@Test
	public void testQueue() {
        int capacity = 5000;
        int count = 10000;
        QueueBuilder<String> builder = QueueBuilder.newBuilder();
        builder.withCapacity(capacity).withSerializer(DefaultSerializerFactory.getInstance().getStringSerializer())
                .withMaxMemory(128).withUnit(Unit.MB);
        final Queue<String> queue = builder.buildQueue();
                
		
        for (int i = 0; i < count; i++) {
            queue.offer("" + i);
        }
        Assert.assertEquals(queue.size(), capacity);
        for (int i = 0; i < capacity; i++) {
            Assert.assertEquals(queue.peek(), "" + i);
            Assert.assertEquals(queue.poll(), "" + i);
        }
        Assert.assertEquals(queue.size(), 0);
	}
}
