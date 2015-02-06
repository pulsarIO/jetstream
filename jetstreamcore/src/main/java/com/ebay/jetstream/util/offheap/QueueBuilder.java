/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import com.ebay.jetstream.util.offheap.queue.OffHeapBlockingQueue;
import com.ebay.jetstream.util.offheap.queue.OffHeapQueue;
import com.ebay.jetstream.util.offheap.queue.QueueDirectMemoryManager;

/**
 * The builder for offheap queue.
 * 
 * By default it configured with 128 MB memory and use 8192 as page size.
 * 
 * @author xingwang
 * 
 * @param <T>
 */
public class QueueBuilder<T> {
    public static enum Unit {
        GB(1 << 30), KB(1 << 10), MB(1 << 20), B(1);

        private long v;

        private Unit(long l) {
            this.v = l;
        }
    }
    /**
     * Create a queue builder.
     * 
     * @return
     */
    public static <T> QueueBuilder<T> newBuilder() {
        return new QueueBuilder<T>();
    }
    private int capacity = Integer.MAX_VALUE;
    private long maxMemory = 128;
    private int pageSize = 8192;
    private OffHeapSerializer<T> serializer;
    private QueueDirectMemoryManager memoryManager;

    private Unit unit = Unit.MB;

    /**
     * Build a blocking queue.
     * 
     * The serializer and capacity can be changed after first build
     * and this method will use the the latest serializer and capacity.
     * 
     * The method can be invoked multiple times, and each time it 
     * will create a new blocking queue which shared same memory 
     * blocks with other queues.
     * 
     * @return
     */
    public synchronized BlockingQueue<T> buildBlockingQueue() {
        checkMemoryManager();
        
        OffHeapBlockingQueue<T> queue = new OffHeapBlockingQueue<T>(memoryManager, capacity, serializer);
        return queue;
    }

    private void checkMemoryManager() {
        if (memoryManager == null) {
            long totalMemoryInBytes = maxMemory * unit.v;
            int pageNum = (int) (totalMemoryInBytes / pageSize);
            memoryManager = new QueueDirectMemoryManager(pageSize, pageNum);
        }
    }
    
    /**
     * Build a queue and the returned queue is not thread-safe.
     * 
     * The serializer and capacity can be changed after first build
     * and this method will use the the latest serializer and capacity.
     * 
     * The method can be invoked multiple times, and each time it 
     * will create a new queue which shared same memory 
     * blocks with other queues.
     * 
     * @return
     */
    public synchronized Queue<T> buildQueue() {
        checkMemoryManager();
        
        OffHeapQueue<T> queue = new OffHeapQueue<T>(memoryManager, capacity, serializer);
        return queue;
    }
    
    /**
     * Clone another builder instance which shared same memory manager.
     * 
     * This provide a mechanism for multiple different type queues shared
     * a big memory. 
     * 
     * @return
     */
    public synchronized <K> QueueBuilder<K> cloneBuilder() {
        QueueBuilder<K> b = new QueueBuilder<K>();
        checkMemoryManager();
        b.memoryManager = this.memoryManager;
        return b;
    }
    
    /**
     * Return the memory manager created or cloned by this builder.
     * 
     * @return
     */
    public OffHeapMemoryManager getOffHeapMemoryManager() {
        return memoryManager;
    }
    
    /**
     * Limit the max items in the queue. The queue can be both bounded by memory
     * and by the capacity.
     * 
     * By default, the queue is memory-bounded.
     * 
     * @param capacity
     * @return
     */
    public QueueBuilder<T> withCapacity(int capacity) {
        this.capacity = capacity;
        return this;
    }

    /**
     * Limit the max memory used by the queue, by default it is 128.
     * 
     * @param maxMemory
     * @return
     */
    public QueueBuilder<T> withMaxMemory(long maxMemory) {
        if (memoryManager != null) {
            throw new IllegalStateException("Can not change max memory after the memory manager initalized");
        }
        this.maxMemory = maxMemory;
        return this;
    }

    /**
     * Configure the page size. By default it is 8192.
     * 
     * The page size should be greater than the item size in bytes. And better
     * to allow one page can include at least 10 items.
     * 
     * @param pageSize
     * @return
     */
    public QueueBuilder<T> withPageSize(int pageSize) {
        if (memoryManager != null) {
            throw new IllegalStateException("Can not change pageSize after the memory manager initalized");
        }
        this.pageSize = pageSize;
        return this;
    }

    /**
     * An optional serializer to optimize the serialization.
     * 
     * @param serializer
     * @return
     */
    public QueueBuilder<T> withSerializer(OffHeapSerializer<T> serializer) {
        this.serializer = serializer;
        return this;
    }

    /**
     * Define the unit for the native memory. By default it is MB.
     * 
     * @param unit
     * @return
     */
    public QueueBuilder<T> withUnit(Unit unit) {
        if (memoryManager != null) {
            throw new IllegalStateException("Can not change unit after the memory manager initalized");
        }
        this.unit = unit;
        return this;
    }
}
