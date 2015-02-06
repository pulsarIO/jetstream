/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.queue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.util.offheap.OffHeapMemoryManager;

/**
 * A native memory page cache. It create a big virtual native memory space by
 * using a list of native buffers.
 * 
 * It is thread safe and can be shared by multiple offheap queues.
 * 
 * @author xingwang
 * 
 */
public class QueueDirectMemoryManager implements OffHeapMemoryManager {
    private static final byte FLAGS_FREE = 0;
    private static final byte FLAGS_USED = 1;
    public static final int LENGTH_HEADER_LENGTH = 4;
    public static final int NULL_PAGE = -1;
    public static final int PAGE_HEADER_LENGTH = 5;

    private int freeList = NULL_PAGE;
    private int freePageCount;
    private final int maxPageNum;
    private int pageIndex = 0;
    private final ByteBuffer[] pages;
    private final int pageSize;
    private final AtomicLong oomCounter = new AtomicLong(0);

    /**
     * Specify pageSize for native buffer length. Use maxPageNum to make it
     * memory bounded.
     * 
     * @param pageSize
     * @param maxPageNum
     */
    public QueueDirectMemoryManager(int pageSize, int maxPageNum) {
        if (pageSize < 8) {
            throw new IllegalArgumentException("Page size should at least 8");
        }
        this.maxPageNum = maxPageNum;
        this.pageSize = pageSize;
        pages = new ByteBuffer[maxPageNum];
    }

    public boolean free(int pageAddress) {
        synchronized (this) {
            if (pages[pageAddress].get(4) != FLAGS_USED) {
                return false;
            }
            pages[pageAddress].putInt(0, freeList);
            pages[pageAddress].put(4, FLAGS_FREE);
            freeList = pageAddress;
            freePageCount++;
        }
        return true;
    }

    public int getAllocatedPage() {
        return pageIndex;
    }

    public ByteBuffer getByteBuffer(int pageIndex) {
        return pages[pageIndex];
    }

    @Override
    public long getFreeMemory() {
        return ((long) freePageCount) * pageSize;
    }

    public int getFreePageNumber() {
        return freePageCount;
    }

    @Override
    public long getMaxMemory() {
        return ((long) maxPageNum) * pageSize;
    }

    public int getNextPage(int curPage) {
        return pages[curPage].getInt(0);
    }

    public int getPageSize() {
        return pageSize;
    }

    @Override
    public long getReservedMemory() {
        return ((long) pageIndex) * pageSize;
    }

    @Override
    public long getUsedMemory() {
        return ((long) (pageIndex - freePageCount)) * pageSize;
    }

    public int malllocFirstPage() {
        synchronized (this) {
            if (maxPageNum > 0 && (freePageCount + (maxPageNum - pageIndex) < 1)) {
                return NULL_PAGE;
            }
            return malloc();
        }
    }
    
    public int malllocPages(int pageNum, int prePage) {
        synchronized (this) {
            if (maxPageNum > 0 && (freePageCount + (maxPageNum - pageIndex) < pageNum)) {
                return NULL_PAGE;
            }
            int p = prePage;
            while (pageNum > 0) {
                int newPage = malloc();
                if (newPage == NULL_PAGE) {
                    p = prePage;
                    int n = pages[p].getInt(0);
                    while (n != NULL_PAGE) {
                        p = n;
                        n = pages[p].getInt(0);
                        free(p);
                    }
                    
                    return NULL_PAGE;
                }
                pages[p].putInt(0, newPage);
                p = newPage;
                pageNum--;
            }
            return prePage;
        }
    }

    private int malloc() {
        int pageAddress = NULL_PAGE;
        synchronized (this) {
            if (freeList == -1) {
                pageAddress = pageIndex;
                try {
                    pages[pageIndex] = ByteBuffer.allocateDirect(pageSize);
                    pageIndex++;
                } catch (Throwable ex) {
                    oomCounter.incrementAndGet();
                    return NULL_PAGE;
                }
            } else {
                freePageCount--;
                pageAddress = freeList;
                int next = pages[freeList].getInt(0);
                freeList = next;
            }
            pages[pageAddress].putInt(0, NULL_PAGE);
            // Add a byte to indicate it is used.
            pages[pageAddress].put(4, FLAGS_USED);
        }
        return pageAddress;
    }

    @Override
    public long getOOMErrorCount() {
        return oomCounter.get();
    }
}
