/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.transport.netty.eventconsumer.EventProducerSessionHandler;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This is a Netty call callback. Event producers install this handler and this class gets invoked by MINA when
 *          an activity associated with a consumer is completed.
 * 
 */

@Sharable
public class EventConsumerSessionHandler extends ChannelDuplexHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private final LongCounter m_totalMsgsSent = new LongCounter();
	private final LongCounter m_totalBytesSent = new LongCounter();
	private final LongCounter m_avgBytesPerMessage = new LongCounter();
	private final AtomicLong m_minBytesWrittenPerMessage = new AtomicLong(0);
	private final AtomicLong m_maxBytesWrittenPerMessage = new AtomicLong(0);

	private EventProducer m_ep = null;

    /**
	 * @param sm
	 */
	public EventConsumerSessionHandler(EventProducer sm) {
		m_ep = sm;
	}
	
	 @Override
	    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
	            SocketAddress localAddress, ChannelPromise promise) throws Exception {
		 
		 	printInfo("Sender Session established with host - "
					+ ((InetSocketAddress) remoteAddress).getHostName());

		 	super.connect(ctx, remoteAddress, localAddress, promise);
	    }

	    /**
	     * Calls {@link ChannelHandlerContext#disconnect(ChannelPromise)} to forward
	     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
	     *
	     * Sub-classes may override this method to change behavior.
	     */
	    @Override
	    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise)
	            throws Exception {
	    	LOGGER.error( "Session closed to host - "
					+ ctx.channel().remoteAddress());
	    	
	    	m_ep.removeSession(ctx);
			
	        super.disconnect(ctx, promise);
	    }

		    
	
	/**
	 * @return the avgBytesPerMessage
	 */
	public long getAvgBytesPerMessage() {
		return m_avgBytesPerMessage.get();
	}

	/**
	 * @return the maxBytesWritten
	 */
	public long getMaxBytesWrittenPerMessage() {
		return m_maxBytesWrittenPerMessage.get();
	}

	/**
	 * @return the minBytesWritten
	 */
	public long getMinBytesWrittenPerMessage() {
		return m_minBytesWrittenPerMessage.get();
	}

	/**
	 * @return the m_totalSendCnt
	 */
	public long getTotalBytesSent() {
		return m_totalBytesSent.get();
	}

	/**
	 * @return the m_totalSendCnt
	 */
	public long getTotalMsgsSent() {
		return m_totalMsgsSent.get();
	}

	
	/**
	 * @param message
	 */
	private void printInfo(String message) {
		LOGGER.info( message);
	}

	public void reset() {
		m_totalMsgsSent.reset();
		m_totalBytesSent.reset();
	}

	
	/**
   * 
   */
	public void shutdown() {

	}

	
		
    @Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (EventProducerSessionHandler.BATCH_START_EVENT == msg
                || EventProducerSessionHandler.BATCH_END_EVENT == msg) {
            return;
        }
    	JetstreamMessage tm = (JetstreamMessage) msg;
		m_ep.onMessage(tm); // now dispatch to our
		
	}

    /**
     * Calls {@link ChannelHandlerContext#write(Object)} to forward
     * to the next {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     *
     * Sub-classes may override this method to change behavior.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ctx.write(msg, promise);
    }
    
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        printInfo("Send Session created to host - "
                + ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName());
        printInfo("Send Session config -" + ctx.channel().config().getOptions());
        super.channelActive(ctx);
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    	String message = "Channel Inactive ";
		message += " - ";
		message += ((InetSocketAddress) ctx.channel().remoteAddress())
				.getAddress().getHostAddress();
		
		if (ctx.channel().isActive())
			ctx.channel().close();
		
		LOGGER.warn( message);
	
		m_ep.removeSession(ctx);
		
        ctx.fireChannelInactive();
    }
    
    
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
		
		LOGGER.error( cause.getLocalizedMessage(), cause);
		
		m_ep.removeSession(ctx);
		
		if (ctx.channel().isActive())
			ctx.channel().close();
		
    }
    
    
}
