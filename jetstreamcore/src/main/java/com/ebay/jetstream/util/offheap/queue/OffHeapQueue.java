/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.queue;

import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.Iterator;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;
import com.ebay.jetstream.util.offheap.serializer.DefaultSerializerFactory;

/**
 * An offheap queue which store object on java natvie heap. The serialization
 * use java default serialization when no serializer specified.
 * 
 * This is not a thread safe implementation. For thread safe, please use
 * OffHeapBlockingQueue.
 * 
 * @author xingwang
 * 
 * @param <V>
 */
public class OffHeapQueue<V> extends AbstractQueue<V> {
    private static final ThreadLocal<ByteBuffer> tmpBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(1024);
        }
    };

    protected final int capacity;
    private long end;
    private long head;
    private final QueueDirectMemoryManager memoryManager;
    private long offered;
    private final OffHeapSerializer<V> serializer;
    private long taked;
    private boolean initialized = false;

    /**
     * Use capacity to make the queue bounded.
     * 
     * @param pageCache
     * @param capacity
     */
    public OffHeapQueue(QueueDirectMemoryManager pageCache, int capacity, OffHeapSerializer<V> serializer) {
        if (serializer != null) {
            this.serializer = serializer;
        } else {
            this.serializer = DefaultSerializerFactory.getInstance().createObjectSerializer();
        }
        this.capacity = capacity;
        this.memoryManager = pageCache;
        int initPage = pageCache.malllocFirstPage();
        if (initPage == QueueDirectMemoryManager.NULL_PAGE) {
            throw new IllegalStateException("Not enough memory");
        }
        initialized = true;
        long address = initPage;
        address = (address << 32) | QueueDirectMemoryManager.PAGE_HEADER_LENGTH;
        head = end = address;
    }

    public ByteBuffer allocate(int length) {
        ByteBuffer buffer = tmpBuffer.get();
        buffer.clear();
        if (buffer.capacity() < length) {
            buffer = ByteBuffer.allocate(length);
            tmpBuffer.set(buffer);
        }
        buffer.limit(length);
        return buffer;
    }

    protected V dequeue() {
        if (head == end) {
            return null;
        }
        int pageSize = memoryManager.getPageSize();
        int offset = (int) (head & 0x00000000ffffffffL);
        int pageIndex = (int) ((head >>> 32) & 0x00000000ffffffffL);
        int length;
        int curPage = pageIndex;
        boolean firstPageReleased = false;
        if (offset <= pageSize - QueueDirectMemoryManager.LENGTH_HEADER_LENGTH) {
            byte byte0 = memoryManager.getByteBuffer(pageIndex).get(offset);
            byte byte1 = memoryManager.getByteBuffer(pageIndex).get(offset + 1);
            byte byte2 = memoryManager.getByteBuffer(pageIndex).get(offset + 2);
            byte byte3 = memoryManager.getByteBuffer(pageIndex).get(offset + 3);
            length = makeInt(byte0, byte1, byte2, byte3);
            offset = offset + QueueDirectMemoryManager.LENGTH_HEADER_LENGTH;
            if (offset == pageSize) {
                curPage = memoryManager.getNextPage(pageIndex);
                offset = QueueDirectMemoryManager.PAGE_HEADER_LENGTH;
                firstPageReleased = true;
            }
        } else {
            ByteBuffer buffer1 = memoryManager.getByteBuffer(pageIndex);
            ByteBuffer buffer2 = memoryManager.getByteBuffer(memoryManager.getNextPage(pageIndex));
            byte byte0 = (offset < pageSize) ? buffer1.get(offset) : buffer2.get(offset - pageSize
                    + QueueDirectMemoryManager.PAGE_HEADER_LENGTH);
            byte byte1 = (offset + 1 < pageSize) ? buffer1.get(offset + 1) : buffer2.get(offset + 1 - pageSize
                    + QueueDirectMemoryManager.PAGE_HEADER_LENGTH);
            byte byte2 = (offset + 2 < pageSize) ? buffer1.get(offset + 2) : buffer2.get(offset + 2 - pageSize
                    + QueueDirectMemoryManager.PAGE_HEADER_LENGTH);
            byte byte3 = buffer2.get(offset + 3 - pageSize + QueueDirectMemoryManager.PAGE_HEADER_LENGTH);
            length = makeInt(byte0, byte1, byte2, byte3);
            curPage = memoryManager.getNextPage(pageIndex);
            firstPageReleased = true;
            offset = offset + QueueDirectMemoryManager.LENGTH_HEADER_LENGTH
                    + QueueDirectMemoryManager.PAGE_HEADER_LENGTH - pageSize;
        }

        ByteBuffer readBuffer = null;
        int pos = 0;
        try {
            readBuffer = allocate(length);
            pos = readBuffer.position();
        } finally {
            if (firstPageReleased) {
                memoryManager.free(pageIndex);
            }
            int c = 0;
            int curOffSet = offset;
            ByteBuffer buf = memoryManager.getByteBuffer(curPage);
            while (c < length) {
                if (curOffSet == pageSize) {
                    int nextpage = memoryManager.getNextPage(curPage);
                    memoryManager.free(curPage);
                    curPage = nextpage;
                    curOffSet = QueueDirectMemoryManager.PAGE_HEADER_LENGTH;
                    buf = memoryManager.getByteBuffer(curPage);
                }

                if (readBuffer != null) {
                    readBuffer.put(pos + c, buf.get(curOffSet));
                }
                curOffSet++;
                c++;
            }
            readBuffer.limit(pos + c);
            long newHeader = curPage;
            newHeader = curOffSet | (newHeader << 32);

            head = newHeader;

            taked++;
        }
        
        readBuffer.position(pos);
        return deserialize(readBuffer, pos, length);
    }

    private V deserialize(ByteBuffer buf, int pos, int length) {
        return this.serializer.deserialize(buf, pos, length);
    }

    public boolean enqueue(V o) {
        ByteBuffer buffer = serialize(o);
        int startPos = buffer.position();
        int bufferLength = buffer.limit() - buffer.position();
        int length = bufferLength + QueueDirectMemoryManager.LENGTH_HEADER_LENGTH;
        int pageSize = memoryManager.getPageSize();

        int offset;
        int pageIndex;
        int curPage;

        offset = (int) (end & 0x00000000ffffffffL);
        pageIndex = (int) ((end >>> 32) & 0x00000000ffffffffL);
        curPage = pageIndex;
        int t = offset + length;
        if (t > pageSize) {
            t = t - pageSize;
            int newPages = (t / (pageSize - QueueDirectMemoryManager.PAGE_HEADER_LENGTH))
                    + (t % (pageSize - QueueDirectMemoryManager.PAGE_HEADER_LENGTH) == 0 ? 0 : 1);
            if (memoryManager.malllocPages(newPages, pageIndex) == QueueDirectMemoryManager.NULL_PAGE) {
                return false;
            }
        }

        int c = 0;
        int curOffSet = offset;
        ByteBuffer buf = memoryManager.getByteBuffer(curPage);
        while (c < QueueDirectMemoryManager.LENGTH_HEADER_LENGTH) {
            if (curOffSet == pageSize) {
                curPage = memoryManager.getNextPage(curPage);
                curOffSet = QueueDirectMemoryManager.PAGE_HEADER_LENGTH;
                buf = memoryManager.getByteBuffer(curPage);
            }

            buf.put(curOffSet, getIntByte(bufferLength, c));

            curOffSet++;
            c++;
        }

        c = 0;
        while (c < bufferLength) {
            if (curOffSet == pageSize) {
                curPage = memoryManager.getNextPage(curPage);
                curOffSet = QueueDirectMemoryManager.PAGE_HEADER_LENGTH;
                buf = memoryManager.getByteBuffer(curPage);
            }

            buf.put(curOffSet, buffer.get(startPos + c));

            curOffSet++;
            c++;
        }

        long newEnd = curPage;
        newEnd = curOffSet | (newEnd << 32);

        end = newEnd;
        offered++;
        return true;
    }

    protected V firstItem() {
        if (head == end) {
            return null;
        }
        int pageSize = memoryManager.getPageSize();
        int offset = (int) (head & 0x00000000ffffffffL);
        int pageIndex = (int) ((head >>> 32) & 0x00000000ffffffffL);
        int length;
        int curPage = pageIndex;
        if (offset <= pageSize - QueueDirectMemoryManager.LENGTH_HEADER_LENGTH) {
            byte byte0 = memoryManager.getByteBuffer(pageIndex).get(offset);
            byte byte1 = memoryManager.getByteBuffer(pageIndex).get(offset + 1);
            byte byte2 = memoryManager.getByteBuffer(pageIndex).get(offset + 2);
            byte byte3 = memoryManager.getByteBuffer(pageIndex).get(offset + 3);
            length = makeInt(byte0, byte1, byte2, byte3);
            offset = offset + QueueDirectMemoryManager.LENGTH_HEADER_LENGTH;
            if (offset == pageSize) {
                curPage = memoryManager.getNextPage(pageIndex);
                offset = QueueDirectMemoryManager.PAGE_HEADER_LENGTH;
            }
        } else {
            ByteBuffer buffer1 = memoryManager.getByteBuffer(pageIndex);
            ByteBuffer buffer2 = memoryManager.getByteBuffer(memoryManager.getNextPage(pageIndex));
            byte byte0 = (offset < pageSize) ? buffer1.get(offset) : buffer2.get(offset - pageSize
                    + QueueDirectMemoryManager.PAGE_HEADER_LENGTH);
            byte byte1 = (offset + 1 < pageSize) ? buffer1.get(offset + 1) : buffer2.get(offset + 1 - pageSize
                    + QueueDirectMemoryManager.PAGE_HEADER_LENGTH);
            byte byte2 = (offset + 2 < pageSize) ? buffer1.get(offset + 2) : buffer2.get(offset + 2 - pageSize
                    + QueueDirectMemoryManager.PAGE_HEADER_LENGTH);
            byte byte3 = buffer2.get(offset + 3 - pageSize + QueueDirectMemoryManager.PAGE_HEADER_LENGTH);
            length = makeInt(byte0, byte1, byte2, byte3);
            curPage = memoryManager.getNextPage(pageIndex);
            offset = offset + QueueDirectMemoryManager.LENGTH_HEADER_LENGTH
                    + QueueDirectMemoryManager.PAGE_HEADER_LENGTH - pageSize;
        }

        ByteBuffer readBuffer = allocate(length);
        int pos = readBuffer.position();


        int c = 0;
        int curOffSet = offset;
        ByteBuffer buf = memoryManager.getByteBuffer(curPage);
        while (c < length) {
            if (curOffSet == pageSize) {
                int nextpage = memoryManager.getNextPage(curPage);
                curPage = nextpage;
                curOffSet = QueueDirectMemoryManager.PAGE_HEADER_LENGTH;
                buf = memoryManager.getByteBuffer(curPage);
            }

            readBuffer.put(pos + c, buf.get(curOffSet));
            curOffSet++;
            c++;
        }

        readBuffer.limit(pos + c);
        readBuffer.position(pos);
        return deserialize(readBuffer, pos, length);
    }

    private byte getIntByte(int length, int c) {
        return (byte) ((length >>> (c * 8)) & 0xff);
    }

    @Override
    /**
     * Not supported.
     */
    public Iterator<V> iterator() {
        throw new UnsupportedOperationException();
    }

    private int makeInt(byte b0, byte b1, byte b2, byte b3) {
        return ((((b3 & 0xff) << 24) | ((b2 & 0xff) << 16) | ((b1 & 0xff) << 8) | ((b0 & 0xff) << 0)));
    }

    @Override
    public boolean offer(V o) {
        if (size() + 1 > capacity) {
            return false;
        }
        enqueue(o);
        return true;
    }

    @Override
    public V peek() {
        return firstItem();
    }

    @Override
    public V poll() {
        return dequeue();
    }

    private ByteBuffer serialize(V o) {
        return this.serializer.serialize(o);
    }

    @Override
    public int size() {
        return (int) (offered - taked);
    }
    
    protected void finalize() {
        if (initialized) {
            clear();
            int pageAddress = (int) ((head >>> 32) & 0x00000000ffffffffL);
            if (pageAddress != QueueDirectMemoryManager.NULL_PAGE) {
                memoryManager.free(pageAddress);
            }
        }
    }
}
