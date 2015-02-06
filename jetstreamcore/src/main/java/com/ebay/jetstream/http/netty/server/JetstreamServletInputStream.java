/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpRequest;

import java.io.IOException;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;

public class JetstreamServletInputStream extends ServletInputStream {
    private ByteBuf m_buf;

    public JetstreamServletInputStream(HttpRequest req) {
        m_buf = ((FullHttpMessage) req).content();
    }

    @Override
    public int available() throws IOException {
        return m_buf.readableBytes();

    }

    @Override
    public void close() throws IOException {
        m_buf.clear();
    }

    @Override
    public void mark(int readlimit) {
        m_buf.markReaderIndex();
    }

    @Override
    public int read() throws IOException {
        if (m_buf.readableBytes() == 0)
            return -1;

        return m_buf.readByte() & 0x00ff;
    }

    @Override
    public int read(byte[] b) throws IOException {

        if (m_buf.readableBytes() == 0)
            return -1;

        if (b.length <= m_buf.readableBytes()) {
            m_buf.readBytes(b);
            return b.length;
        }

        else {
            int bytesRead = m_buf.readableBytes();
            m_buf.readBytes(b, 0, bytesRead);
            return bytesRead;

        }

    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (m_buf.readableBytes() == 0)
            return -1;

        int actuallen;

        if (m_buf.readableBytes() >= len) {
            actuallen = len;
            m_buf.readBytes(b, off, len);
        } else {
            actuallen = m_buf.readableBytes();
            m_buf.readBytes(b, off, actuallen);
        }
        return actuallen;

    }

    @Override
    public void reset() throws IOException {
        m_buf.resetReaderIndex();
    }

    @Override
    public long skip(long n) throws IOException {
        if (n < 0)
            return 0;
        if (m_buf.readableBytes() == 0)
            return 0;

        if (n <= m_buf.readableBytes()) {
            m_buf.skipBytes((int) n);
            return n;
        } else {
            int actualBytesSkipped = m_buf.readableBytes();
            m_buf.skipBytes(actualBytesSkipped);
            return actualBytesSkipped;
        }
    }

    @Override
    public boolean isFinished() {
        return m_buf.readableBytes() == 0;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void setReadListener(ReadListener arg0) {
        
    }

}
