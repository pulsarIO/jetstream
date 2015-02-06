/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.compression;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import com.ebay.jetstream.xmlser.XSerializable;



public class MessageDecompressionHandler extends LengthFieldBasedFrameDecoder implements XSerializable 
{


	byte[] m_tmpBuf;
	boolean m_allocBuf = false;
	int m_tmpBufSz = 250000;
	
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	
	/* if allocBuf is set to true, we are expected to allocate buf in decode everytine decode is called.
	 * use this model when the MessageDecompressionHandler is shared between threads. If set to false, the
	 * temp buffer is allocated once at begining (this is thread unsafe but performs better)
	 */
	public MessageDecompressionHandler(boolean allocBuf, int bufSz) {
		super(1024*1024, 0, 4, 0, 4);
			
		m_allocBuf = allocBuf;
		m_tmpBufSz = bufSz;
		if (!m_allocBuf)
			m_tmpBuf = new byte[bufSz];
			
	}
	
 	
	@Override
	 protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception
	{

		ByteBuf frame = (ByteBuf) super.decode(ctx, in);

		if (frame == null) {
			return null;
		}

		byte[] uncompressedbuf;
		
		if (m_allocBuf)
			uncompressedbuf = new byte[m_tmpBufSz];
		else
			uncompressedbuf = m_tmpBuf;
		
		int framelen = frame.readableBytes();
		
		int len = 0;

		try {

			len = Snappy.rawUncompress(frame.readBytes(framelen).array(), 0, framelen, uncompressedbuf, 0);
			
		} catch (Throwable t) {

			LOGGER.debug( "Failed to uncompress - " + t.getLocalizedMessage());

			frame.release();
			return null;
		}
		
		frame.release();
			
		ByteBuf buf = ctx.alloc().directBuffer(len);
		
		return buf.writeBytes(uncompressedbuf, 0, len);		
								
	}

	public static void initSnappy() {

		byte[] compressed = null;
		long startTime = System.currentTimeMillis();

		try {

			String testMsg = "test";

			compressed = Snappy.rawCompress(testMsg.getBytes(), testMsg.length());

		} catch (UnsupportedEncodingException e) {

			LOGGER.error( "failed to compress using Snappy - " + e.toString());

		} catch (IOException e) {
			LOGGER.error( "failed to compress using Snappy - " + e.toString());

		}

		byte[] uncompressed = new byte[100];

		int len = 0;

		try {
			len = Snappy.rawUncompress(compressed, 0, compressed.length, uncompressed, 0);
		} catch (Throwable t) {
			LOGGER.error( "failed to uncompress using Snappy - " + t.toString());

		}

	}
		
}
