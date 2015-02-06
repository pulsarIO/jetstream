/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.queue;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;

/**
 * An offheap implementation for blocking queue, it is thread safe. The
 * serialization use java default serialization when no serializer specified.
 * 
 * @author xingwang
 * 
 * @param <V>
 */
public class OffHeapBlockingQueue<V> extends OffHeapQueue<V> implements BlockingQueue<V>{

    private final AtomicInteger count = new AtomicInteger(0);
    private final ReentrantLock putLock = new ReentrantLock();
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private final Condition notFull = putLock.newCondition();

    public OffHeapBlockingQueue(QueueDirectMemoryManager pageCache, OffHeapSerializer<V> serializer) {
        this(pageCache, Integer.MAX_VALUE, serializer);
    }
    
	public OffHeapBlockingQueue(QueueDirectMemoryManager pageCache,
			int capacity, OffHeapSerializer serializer) {
		super(pageCache, capacity, serializer);
	}

    @Override
    public int drainTo(Collection<? super V> collection) {
        return drainTo(collection, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super V> collection, int maxElements) {
        if (count.get() == 0)
            return 0;
        int n = Math.min(maxElements, count.get());
        int c = 0;
        while (c < n) {
            V v = this.poll();
            if (v != null) {
                c++;
                collection.add(v);
            } else {
                break;
            }
        }
        return c;
    }

    @Override
    public boolean offer(V e) {
        if (e == null)
            throw new NullPointerException();
        if (count.get() == capacity)
            return false;
        int c = -1;
        putLock.lock();
        try {
            if (count.get() < capacity) {
                if (!enqueue(e)) {
                    return false;
                }
                c = count.getAndIncrement();
                if (c + 1 < capacity)
                    notFull.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return c >= 0;
    }

    @Override
    public boolean offer(V e, long timeout, TimeUnit unit) throws InterruptedException {
        if (e == null)
            throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        int c = -1;
        putLock.lockInterruptibly();
        try {
            while (count.get() == capacity) {
                if (nanos <= 0)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }
            if (!enqueue(e)) {
                return false;
            }
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
        return true;
    }
    
    @Override
    public V peek() {
        if (count.get() == 0)
            return null;
        takeLock.lock();
        try {
            return firstItem();
        } finally {
            takeLock.unlock();
        }
    }


    @Override
    public V poll() {
        if (count.get() == 0)
            return null;
        V x = null;
        int c = -1;
        takeLock.lock();
        try {
            if (count.get() > 0) {
                x = dequeue();
                c = count.getAndDecrement();
                if (c > 1)
                    notEmpty.signal();
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity)
            signalNotFull();
        return x;
    }

    @Override
    public V poll(long timeout, TimeUnit unit) throws InterruptedException {
        V x = null;
        int c = -1;
        long nanos = unit.toNanos(timeout);
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            x = dequeue();
            c = count.getAndDecrement();
            if (c > 1)
                notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
        if (c == capacity)
            signalNotFull();
        return x;
    }

    @Override
    public void put(V e) throws InterruptedException {
        if (e == null)
            throw new NullPointerException();
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        putLock.lockInterruptibly();
        try {
            while (count.get() == capacity) {
                notFull.await();
            }
            if (!enqueue(e)) {
                throw new IllegalStateException("Memory used up");
            }
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
    }

    @Override
    public int remainingCapacity() {
        return capacity - count.get();
    }

    @Override
    public V take() throws InterruptedException {
        V x;
        int c = -1;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                notEmpty.await();
            }
            x = dequeue();
            c = count.getAndDecrement();
            if (c > 1) {
                notEmpty.signal();
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return x;
    }
    
    @Override
    public int size() {
        return count.get();
    }
	
    private void signalNotEmpty() {
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }
	
    private void signalNotFull() {
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }
}
