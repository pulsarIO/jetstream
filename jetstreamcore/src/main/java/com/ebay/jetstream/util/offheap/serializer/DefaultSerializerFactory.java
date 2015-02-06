/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util.offheap.serializer;

import com.ebay.jetstream.util.offheap.OffHeapSerializer;

/**
 * An default serializer factory.
 * 
 * @author xingwang
 *
 */
public class DefaultSerializerFactory {
    private final IntSerializer intSerilaizer = new IntSerializer();
    private final LongSerializer longSerilaizer = new LongSerializer();
    private final StringSerializer stringSerilaizer = new StringSerializer();
    
    private static final DefaultSerializerFactory INSTANCE = new DefaultSerializerFactory();
    
    public static DefaultSerializerFactory getInstance() {
        return INSTANCE;
    }
    
    public OffHeapSerializer<Integer> getIntSerializer() {
        return intSerilaizer;
    }
    
    public OffHeapSerializer<String> getStringSerializer() {
        return stringSerilaizer;
    }
    
    public OffHeapSerializer<Long> getLongSerializer() {
        return longSerilaizer;
    }
    
    public <T> OffHeapSerializer<T> createObjectSerializer() {
        return new ObjectSerializer<T>();
    }
    
    public <T> OffHeapSerializer<T> createKryoSerializer() {
        return new KryoSerializer<T>();
    }
}
