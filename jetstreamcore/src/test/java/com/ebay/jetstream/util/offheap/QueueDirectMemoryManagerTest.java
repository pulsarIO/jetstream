/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

import junit.framework.Assert;

import org.junit.Test;

import com.ebay.jetstream.util.offheap.queue.QueueDirectMemoryManager;

public class QueueDirectMemoryManagerTest {
    @Test
    public void test() {
        int pageNum = 3000;
        int pageSize = 8192;
        QueueDirectMemoryManager m = new QueueDirectMemoryManager(pageSize, pageNum);
        
        for (int i = 1 ; i < pageNum + 5; i++) {
            if (m.getOOMErrorCount() > 100) {
                break;
            }
            int first = m.malllocFirstPage();
            Assert.assertEquals(m.getUsedMemory(), pageSize);
            if (i < pageNum) {
                int address = m.malllocPages(i, first); 
                if (address == QueueDirectMemoryManager.NULL_PAGE) {
                    Assert.assertEquals(m.getUsedMemory(), pageSize);
                } else {
                    Assert.assertEquals(address, first);
                    Assert.assertEquals(m.getUsedMemory(), pageSize * (i + 1));
                }
            } else {
                Assert.assertEquals(m.malllocPages(i, first), QueueDirectMemoryManager.NULL_PAGE);
            }
            
            int curPage = first;
            int nextPage;
            while (curPage != QueueDirectMemoryManager.NULL_PAGE) {
                nextPage = m.getNextPage(curPage);
                m.free(curPage);
                curPage = nextPage;
            }
            
            Assert.assertEquals(m.getUsedMemory(), 0);
        }
    }
}
