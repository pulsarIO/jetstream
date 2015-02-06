/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventconsumer;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This is a callback class required by Netty to provide notification of session state. This is installed by the
 *          event consumer.
 * 
 */

@Sharable
public class EventProducerSessionHandler extends ChannelDuplexHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	// This is marker event for receive a bunch of messages from transport.
	public static final Object BATCH_START_EVENT = new Object();
	public static final Object BATCH_END_EVENT = new Object();
	private static final AttributeKey<List<JetstreamMessage>> BATCH_CONTAINER = AttributeKey.valueOf("inbound_batch_container");
	private static final AttributeKey<Boolean> BATCH_FLAG = AttributeKey.valueOf("inbound_batch_flag");
	private static final int MAX_INBOUND_BATCH_SIZE = 256;
	
	private EventConsumer m_eventConsumer = null;
	
	
	/**
	 * @param ec
	 */
	public EventProducerSessionHandler(EventConsumer ec) {
		m_eventConsumer = ec;
		

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#channelConnected(org.jboss
	 * .netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.ChannelStateEvent)
	 */

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		printInfo("Rcvr Session created to host - "
				+ ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName());
		printInfo("Rcvr Session config -" + ctx.channel().config().getOptions());
		
		m_eventConsumer.registerChannel(ctx.channel());
		super.channelActive(ctx);
	}

	/**
	 * Calls {@link ChannelHandlerContext#fireChannelInactive()} to forward
	 * to the next {@link ChannelInboundHandler} in the {@link ChannelPipeline}.
	 *
	 * Sub-classes may override this method to change behavior.
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		printInfo("EventProducerSessionHandler -> session closed to host - "
				+ ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName());

		m_eventConsumer.unregisterChannel(ctx.channel());

		super.channelInactive(ctx);

	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.jboss.netty.channel.SimpleChannelHandler#messageReceived(org.jboss
	 * .netty.channel.ChannelHandlerContext,
	 * org.jboss.netty.channel.MessageEvent)
	 */

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
	    if (EventProducerSessionHandler.BATCH_START_EVENT == msg) {
	        ctx.attr(BATCH_FLAG).set(Boolean.TRUE);
	        return;
	    }
	    
	    if (EventProducerSessionHandler.BATCH_END_EVENT == msg) {
	        ctx.attr(BATCH_FLAG).remove();
	        List<JetstreamMessage> msgs = ctx.attr(BATCH_CONTAINER).get();
	        if (msgs != null && !msgs.isEmpty()) {
                try {
                    m_eventConsumer.receive(msgs);
                } finally {
                    msgs.clear();
                }
	        }
	        return;
	    }
	    
	    Boolean flag = ctx.attr(BATCH_FLAG).get();
	    if (flag != null) {
	        List<JetstreamMessage> msgs = ctx.attr(BATCH_CONTAINER).get();
    	    if (msgs == null) {
    	        ctx.attr(BATCH_CONTAINER).setIfAbsent(new ArrayList<JetstreamMessage>(MAX_INBOUND_BATCH_SIZE));
    	        msgs = ctx.attr(BATCH_CONTAINER).get();
    	    }
            msgs.add((JetstreamMessage) msg);
            if (msgs.size() == MAX_INBOUND_BATCH_SIZE) {
                try {
                    m_eventConsumer.receive(msgs);
                } finally {
                    msgs.clear();
                }
            }
	    } else {
            JetstreamMessage tm = (JetstreamMessage) msg;
            m_eventConsumer.receive(tm); // now dispatch upstream
	    }
	}



	/**
	 * @param message
	 */
	private void printInfo(String message) {
		LOGGER.info( message);
	}
	
	
	
	@Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
		
		LOGGER.error( cause.getLocalizedMessage(), cause);
		
		m_eventConsumer.unregisterChannel(ctx.channel());
		
		if (ctx.channel().isActive())
			ctx.channel().close();
		
    }
	
		
}
