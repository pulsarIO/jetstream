/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.compression;


import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventProducer;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.ExtendedChannelPromise;
import com.ebay.jetstream.xmlser.XSerializable;


public class MessageCompressionHandler extends ChannelOutboundHandlerAdapter implements XSerializable
{

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	private LongCounter m_totalMessagesCompressed = new LongCounter();
	private LongCounter m_totalMessagesDropped = new LongCounter();
	
	public MessageCompressionHandler() {
	}
	
	    
    
    /**
     * Invoked when {@link Channel#write(Object)} is called.
     */
	 public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
	
    	try {
    		
    		Attribute<Boolean> attr = ctx.channel().attr(EventProducer.m_eckey);
    		
    		Boolean enableCompression = attr.get();
    		
    		if ((enableCompression != null) && (enableCompression == true)) {
    		
	    		ByteBuf chbuf = (ByteBuf) msg;
	
	    		int msglen = chbuf.readableBytes();
	    		ExtendedChannelPromise epromise = (ExtendedChannelPromise) promise;
	    		epromise.setRawBytes(msglen);
	    		
	    		byte[] compressed = Snappy.rawCompress(chbuf.readBytes(msglen).array(), msglen);
	    		
	    		epromise.setCompressedBytes(compressed.length + 4);
	    		chbuf.release(); // need to release the original buffer - do I need to check if this this a ref counted buffer
	    		
	    		ByteBuf sendbuf = ctx.alloc().buffer();
	    			    				
	    		sendbuf.writeInt(compressed.length);
	    		sendbuf.writeBytes(compressed);
	    	
	    		ctx.write(sendbuf, promise);
    		
	    		m_totalMessagesCompressed.increment();
	    		
	    		
    		}else{
    			
    			ctx.write(msg, promise);
    			
    		}
	    		

    	} catch (Throwable t) {
    		m_totalMessagesDropped.increment();
    		LOGGER.debug( "Failed to compress message - " + t.getLocalizedMessage());
    		
    	} 
  		      
    }
    
    /**
     * Call this method during init of app - the first call to snappy will take a few secs which will cause event loss if this is
     * not called early on.
     */
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
