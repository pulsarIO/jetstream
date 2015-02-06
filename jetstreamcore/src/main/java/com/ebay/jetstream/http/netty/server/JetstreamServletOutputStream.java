/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

public class JetstreamServletOutputStream extends ServletOutputStream {

    private ByteBufOutputStream m_bufoutputstream;

    /**
     * @param bufoutputstream
     */
    public JetstreamServletOutputStream(ByteBufOutputStream bufoutputstream) {
        m_bufoutputstream = bufoutputstream;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.OutputStream#write(int)
     */
    @Override
    public void write(int b) throws IOException {
        m_bufoutputstream.write(b);

    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void setWriteListener(WriteListener arg0) {
        
    }

}
