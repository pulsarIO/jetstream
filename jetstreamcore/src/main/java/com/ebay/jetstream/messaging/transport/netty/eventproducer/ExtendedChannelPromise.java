/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;

public class ExtendedChannelPromise extends DefaultChannelPromise {

    private int m_writtenSize;
    private int m_rawBytes;
    private int m_compressedBytes;
    public ExtendedChannelPromise(Channel channel) {
        super(channel);
    }

    public int getCompressedBytes() {
        return m_compressedBytes;
    }

    public int getRawBytes() {
        return m_rawBytes;
    }

    public int getWrittenSize() {
        return m_writtenSize;
    }

    public void setCompressedBytes(int compressedBytes) {
        this.m_compressedBytes = compressedBytes;
    }

    public void setRawBytes(int rawBytes) {
        this.m_rawBytes = rawBytes;
    }

    public void setWrittenSize(int writtenSize) {
        this.m_writtenSize = writtenSize;
    }
}
