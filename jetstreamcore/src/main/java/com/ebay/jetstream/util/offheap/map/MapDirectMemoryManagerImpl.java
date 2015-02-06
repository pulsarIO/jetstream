/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.map;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.util.offheap.OffHeapMemoryManager;

public class MapDirectMemoryManagerImpl implements OffHeapMemoryManager, MapDirectMemoryManager {
    private static class Page {
        private static long readLong(ByteBuffer buffer, int pos) {
            return buffer.getLong(pos);
        }

        private static void writeLong(ByteBuffer buf, int pos, long l) {
            buf.putLong(pos, l);
        }

        final ByteBuffer directBuffer;

        public Page(int pageSize, long baseAddress, int blockSize) {
            directBuffer = ByteBuffer.allocateDirect(pageSize);
            directBuffer.limit(directBuffer.capacity());
            int entryCount = pageSize / blockSize;
            for (int i = 0; i < entryCount; i++) {
                int offset = i * blockSize;
                long nextAddress = baseAddress | (i + 1);
                if (i == entryCount - 1) {
                    nextAddress = MapDirectMemoryManager.NULL_ADDRESS;
                }
                writeLong(directBuffer, offset, nextAddress); // next
            }
        }

        public long getNext(int entryIndex, int blockSize) {
            int pos = entryIndex * blockSize;
            return readLong(directBuffer, pos);
        }

        public void setNext(int entryIndex, long address, int blockSize) {
            int pos = entryIndex * blockSize;
            writeLong(directBuffer, pos, address);
        }
    }

    private static final byte FLAGS_FREE = 0;

    private static final byte FLAGS_HEAD = 2;
    private static final byte FLAGS_USED = 1;
    public static final int HEADER_LENGTH = 9;
    private static final int MEMORY_FREE_FLAGS_OFFSET = 8;
    private static final int META_DATA_LENGTH = 32;
    private static final int OFFSET_LEFT = 25;
    private static final int OFFSET_NEXT = 9;
    private static final int OFFSET_RIGHT = 17;
    private static final int OFFSET_TS = 33;
    private final AtomicLong oomCounter = new AtomicLong(0);
    private static final ThreadLocal<ByteBuffer> tmpBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(1024);
        }
    };
    private final int blockSize;
    private final int dataSizePerEntry;
    private long freeListHead = MapDirectMemoryManager.NULL_ADDRESS;
    private int pageIndex;

    private final Page[] pages;
    private final int pageSize; // Not exceed 65535 * ENTRY_SIZE
    private long used = 0;

    public MapDirectMemoryManagerImpl(int pageSize, int pageNum, int blockSize) {
        this.pageSize = pageSize;
        pages = new Page[pageNum];
        this.blockSize = blockSize;
        dataSizePerEntry = blockSize - HEADER_LENGTH;
    }

    private ByteBuffer allocate(int length) {
        ByteBuffer buffer = tmpBuffer.get();
        buffer.clear();
        if (buffer.capacity() < length) {
            buffer = ByteBuffer.allocate(length);
            tmpBuffer.set(buffer);
        }
        buffer.limit(length);
        return buffer;
    }

    @Override
    public ByteBuffer copyKeyBuffer(ByteBuffer buffer) {
        ByteBuffer tmpBuffer = allocate(buffer.limit() - buffer.position());
        for (int i = 0, t = buffer.limit() - buffer.position(); i < t; i++) {
            tmpBuffer.put(buffer.get());
        }
        tmpBuffer.flip();
        return tmpBuffer;
    }

    @Override
    public synchronized void free(long head) {
        if (getByteBuffer(head).get(getLocation(head) + MEMORY_FREE_FLAGS_OFFSET) != FLAGS_HEAD) {
            return;
        }
        getByteBuffer(head).put(getLocation(head) + MEMORY_FREE_FLAGS_OFFSET, FLAGS_USED);
        long p = head;
        while (p != MapDirectMemoryManager.NULL_ADDRESS) {
            long n = getNextBlock(p);
            setNextBlock(p, MapDirectMemoryManager.NULL_ADDRESS);
            freeBlock(p);
            p = n;
        }
    }

    private void freeBlock(long address) {
        if (getByteBuffer(address).get(getLocation(address) + MEMORY_FREE_FLAGS_OFFSET) != FLAGS_USED) {
            return;
        }
        getByteBuffer(address).put(getLocation(address) + MEMORY_FREE_FLAGS_OFFSET, FLAGS_FREE);
        used--;

        setNextBlock(address, freeListHead);
        freeListHead = address;
    }

    private ByteBuffer getByteBuffer(long address) {
        int pageIndex = getPageIndex(address);
        return pages[pageIndex].directBuffer;
    }

    private int getEntryIndex(long address) {
        return (int) (address & 0xffff);
    }

    @Override
    public long getFreeMemory() {
        return  (((long) pageIndex) * (pageSize / blockSize) - used) * blockSize;
    }

    @Override
    public ByteBuffer getKey(long address) {
        int keyLength = getKeyLength(address);
        ByteBuffer readBuffer = allocate(keyLength);
        readKey(address, readBuffer);
        return readBuffer;
    }

    public int getKeyLength(long address) {
        ByteBuffer buffer = getByteBuffer(address);
        int location = getLocation(address);

        return buffer.getInt(location + HEADER_LENGTH + META_DATA_LENGTH + 4);
    }

    @Override
    public long getLeft(long address) {
        return readLong(address, OFFSET_LEFT);
    }

    private int getLocation(long address) {
        int entryIndex = getEntryIndex(address);
        return entryIndex * blockSize;
    }

    @Override
    public long getMaxMemory() {
        return ((long) pages.length) * ((pageSize / blockSize) * blockSize);
    }

    @Override
    public long getNext(long address) {
        return readLong(address, OFFSET_NEXT);
    }

    private long getNextBlock(long address) {
        int pageIndex = getPageIndex(address);
        int entryIndex = getEntryIndex(address);
        return pages[pageIndex].getNext(entryIndex, blockSize);
    }

    private int getPageIndex(long address) {
        return (int) ((address & 0x0000ffffffff0000L) >>> 16);
    }

    @Override
    public long getReservedMemory() {
        return ((long) pageIndex) * pageSize;
    }

    @Override
    public long getRight(long address) {
        return readLong(address, OFFSET_RIGHT);
    }

    @Override
    public long getTimestamp(long address) {
        return readLong(address, OFFSET_TS);
    }

    @Override
    public long getUsedMemory() {
        return ((long) used) * blockSize;
    }

    @Override
    public ByteBuffer getValue(long address) {
        int valueLength = getValueLength(address);
        ByteBuffer readBuffer = allocate(valueLength);
        readValue(address, readBuffer);
        return readBuffer;
    }

    public int getValueLength(long address) {
        ByteBuffer buffer = getByteBuffer(address);
        int location = getLocation(address);

        return buffer.getInt(location + HEADER_LENGTH + META_DATA_LENGTH + 8);
    }

    @Override
    public boolean isKey(long address, ByteBuffer keyBuffer) {
        ByteBuffer buffer = getByteBuffer(address);
        int location = getLocation(address);
        int startPos = keyBuffer.position();
        int length = buffer.getInt(location + HEADER_LENGTH + META_DATA_LENGTH + 4);
        int anotherKeyLength = keyBuffer.limit() - keyBuffer.position();
        if (length != anotherKeyLength) {
            return false;
        }        
        int c = 0;
        int offset = HEADER_LENGTH + META_DATA_LENGTH + 12;
        long p = address;
        while (c < length) {
            if (offset == blockSize) {
                long next = getNextBlock(p);
                buffer = getByteBuffer(next);
                location = getLocation(next);
                p = next;
                offset = HEADER_LENGTH;
            }
            while (offset < blockSize && c < length) {
                if (keyBuffer.get(startPos + c) != buffer.get(location + offset)) {
                    return false;
                }
                offset++;
                c++;
            }
        }

        return true;
    }

    private long malloc() {
        if (freeListHead == MapDirectMemoryManager.NULL_ADDRESS) {
            if (pageIndex == pages.length) {
                return MapDirectMemoryManager.NULL_ADDRESS; // can not allocate.
            } else {
                if (pages[pageIndex] == null) {
                    long pageIndexL = pageIndex;
                    long baseAddress = pageIndexL << 16;
                    try {
                        pages[pageIndex] = new Page(pageSize, baseAddress, blockSize);
                    } catch (Throwable ex) {
                        oomCounter.incrementAndGet();
                        return MapDirectMemoryManager.NULL_ADDRESS; // OOM
                    }
                }
                long pageIndexL = pageIndex;
                long baseAddress = pageIndexL << 16;
                pageIndex++;
                freeListHead = baseAddress;
            }
        }

        used++;
        long allocated = freeListHead;
        int pageIndex = getPageIndex(freeListHead);
        int entryIndex = getEntryIndex(freeListHead);
        pages[pageIndex].directBuffer.put(entryIndex * blockSize + MEMORY_FREE_FLAGS_OFFSET, FLAGS_USED);
        freeListHead = pages[pageIndex].getNext(entryIndex, blockSize);
        return allocated;
    }

    private synchronized long malloc(int count) {
        long head = malloc();
        if (head == MapDirectMemoryManager.NULL_ADDRESS) {
            return MapDirectMemoryManager.NULL_ADDRESS;
        }
        setNextBlock(head, MapDirectMemoryManager.NULL_ADDRESS);
        long p = head;
        for (int i = 1; i < count; i++) {
            long next = malloc();

            if (next == MapDirectMemoryManager.NULL_ADDRESS) {
                p = head;
                while (p != MapDirectMemoryManager.NULL_ADDRESS) {
                    long n = getNextBlock(p);
                    setNextBlock(p, MapDirectMemoryManager.NULL_ADDRESS);
                    freeBlock(p);
                    p = n;
                }
                return MapDirectMemoryManager.NULL_ADDRESS;
            }
            setNextBlock(next, MapDirectMemoryManager.NULL_ADDRESS);
            setNextBlock(p, next);
            p = next;
        }
        getByteBuffer(head).put(getLocation(head) + MEMORY_FREE_FLAGS_OFFSET, FLAGS_HEAD);
        return head;
    }

    public void readKey(long address, ByteBuffer buf) {
        ByteBuffer buffer = getByteBuffer(address);
        int startPos = buf.position();
        int location = getLocation(address);

        int length = buffer.getInt(location + HEADER_LENGTH + META_DATA_LENGTH + 4);
        int c = 0;
        int offset = HEADER_LENGTH + META_DATA_LENGTH + 12;
        long p = address;
        while (c < length) {
            if (offset == blockSize) {
                long next = getNextBlock(p);
                buffer = getByteBuffer(next);
                location = getLocation(next);
                p = next;
                offset = HEADER_LENGTH;
            }
            while (offset < blockSize && c < length) {
                buf.put(startPos + c, buffer.get(location + offset));
                offset++;
                c++;
            }
        }
        buf.position(startPos);
    }

    private long readLong(long address, int offset) {
        ByteBuffer buffer = getByteBuffer(address);
        int location = getLocation(address);
        return buffer.getLong(location + offset);
    }

    public void readValue(long address, ByteBuffer buf) {
        ByteBuffer buffer = getByteBuffer(address);
        int location = getLocation(address);
        int startPos = buf.position();
        int length = buffer.getInt(location + HEADER_LENGTH + META_DATA_LENGTH);
        int keyLength = buffer.getInt(location + HEADER_LENGTH + META_DATA_LENGTH + 4);
        int skippedLength = keyLength + 8;
        int c = 0;
        int offset = HEADER_LENGTH + META_DATA_LENGTH + 4;
        long p = address;
        while (c < length) {
            if (offset == blockSize) {
                long next = getNextBlock(p);
                buffer = getByteBuffer(next);
                location = getLocation(next);
                p = next;
                offset = HEADER_LENGTH;
            }
            while (offset < blockSize && c < length) {
                if (c >= skippedLength) {
                    buf.put(startPos + (c - skippedLength), buffer.get(location + offset));
                }
                offset++;
                c++;
            }
        }
        buf.position(startPos);
    }

    @Override
    public long setKeyValue(ByteBuffer key, ByteBuffer value) {
        int keyStartPos = key.position();
        int valueStartPos = value.position();
        int keyLength = key.limit() - key.position();
        int valueLength = value.limit() - value.position();
        int dataLength = keyLength + valueLength;
        int totalLength = keyLength + valueLength + 12 + 32;
        int entryCount = totalLength / dataSizePerEntry;
        if (totalLength % dataSizePerEntry > 0) {
            entryCount++;
        }

        long address = malloc(entryCount);

        if (address == MapDirectMemoryManager.NULL_ADDRESS) {
            return MapDirectMemoryManager.NULL_ADDRESS;
        }

        // Write length first, then all bytes;
        ByteBuffer buffer = getByteBuffer(address);
        int location = getLocation(address);
        buffer.putLong(location + OFFSET_LEFT, MapDirectMemoryManager.NULL_ADDRESS);
        buffer.putLong(location + OFFSET_NEXT, MapDirectMemoryManager.NULL_ADDRESS);
        buffer.putLong(location + OFFSET_RIGHT, MapDirectMemoryManager.NULL_ADDRESS);
        buffer.putLong(location + OFFSET_TS, MapDirectMemoryManager.NULL_ADDRESS);

        buffer.putInt(location + HEADER_LENGTH + META_DATA_LENGTH, dataLength + 8);
        buffer.putInt(location + HEADER_LENGTH + META_DATA_LENGTH + 4, keyLength);
        buffer.putInt(location + HEADER_LENGTH + META_DATA_LENGTH + 8, valueLength);
        int c = 0;
        int offset = HEADER_LENGTH + META_DATA_LENGTH + 12;
        long p = address;
        while (c < dataLength) {
            if (offset == blockSize) {
                long next = getNextBlock(p);
                buffer = getByteBuffer(next);
                location = getLocation(next);
                p = next;
                offset = HEADER_LENGTH;
            }
            while (offset < blockSize && c < dataLength) {
                if (c < keyLength) {
                    buffer.put(location + offset, key.get(keyStartPos + c));
                } else {
                    buffer.put(location + offset, value.get(valueStartPos + c - keyLength));
                }
                offset++;
                c++;
            }
        }

        return address;
    }

    @Override
    public void setLeft(long address, long left) {
        writeLong(address, OFFSET_LEFT, left);
    }

    @Override
    public void setNext(long address, long next) {
        writeLong(address, OFFSET_NEXT, next);
    }

    private void setNextBlock(long address, long next) {
        int pageIndex = getPageIndex(address);
        int entryIndex = getEntryIndex(address);
        pages[pageIndex].setNext(entryIndex, next, blockSize);
    }

    @Override
    public void setRight(long address, long right) {
        writeLong(address, OFFSET_RIGHT, right);
    }

    @Override
    public void setTimestamp(long address, long timestamp) {
        writeLong(address, OFFSET_TS, timestamp);
    }

    private void writeLong(long address, int offset, long value) {
        ByteBuffer buffer = getByteBuffer(address);
        int location = getLocation(address);
        buffer.putLong(location + offset, value);
    }
    
    @Override
    public long getOOMErrorCount() {
        return oomCounter.get();
    }
}