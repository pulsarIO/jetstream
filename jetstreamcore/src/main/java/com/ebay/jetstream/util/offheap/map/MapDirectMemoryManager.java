/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.map;

import java.nio.ByteBuffer;

import com.ebay.jetstream.util.offheap.OffHeapMemoryManager;

public interface MapDirectMemoryManager extends OffHeapMemoryManager {
    long NULL_ADDRESS = -1L;

    ByteBuffer copyKeyBuffer(ByteBuffer buffer);

    void free(long address);

    ByteBuffer getKey(long address);

    ByteBuffer getValue(long address);

    boolean isKey(long address, ByteBuffer keyBuffer);

    long setKeyValue(ByteBuffer keyBuffer, ByteBuffer valueBuffer);

    long getLeft(long address);

    long getNext(long address);

    long getRight(long address);

    long getTimestamp(long address);

    void setLeft(long address, long left);

    void setNext(long address, long next);

    void setRight(long address, long right);

    void setTimestamp(long address, long timestamp);
}
