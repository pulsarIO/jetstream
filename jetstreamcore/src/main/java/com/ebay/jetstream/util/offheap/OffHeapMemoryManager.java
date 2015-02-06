/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap;

/**
 * Base interface for off-heap memory manager.
 * 
 * Provide the memory usage in bytes for each memory manager.
 * 
 * @author xingwang
 */
public interface OffHeapMemoryManager {
    /**
     * Return free memory.
     * 
     * @return
     */
    long getFreeMemory();

    /**
     * Return max usable memory configured for the memory manager.
     * 
     * @return
     */
    long getMaxMemory();

    /**
     * Return the memory it allocated from the native heap.
     * 
     * @return
     */
    long getReservedMemory();
    
    /**
     * Return current used memory.
     * 
     * @return
     */
    long getUsedMemory();
    
    /**
     * Return out of memory count.
     * 
     * @return
     */
    long getOOMErrorCount();
}
