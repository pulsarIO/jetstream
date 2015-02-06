/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.DefaultChannelPromise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.util.Request;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * This is a work Item triggering reading of the controlQueue.
 * An instance of this must be inserted in to dataQueue for every
 * message inserted in to control queue
 */

public class EventConsumerActivationRequest extends Request implements ChannelFutureListener  {

  public static class EventConsumerActivationPromise extends DefaultChannelPromise {

	public EventConsumerActivationPromise(Channel channel) {
		super(channel);
		
	}
	
	public void addListener(ChannelFutureListener listener) {
		super.addListener(listener);
	}
  }
  
  
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
  private final EventProducer m_producer;
  private final EventConsumerInfo m_ecInfo;
  private final long m_maxNettybackLog;
  private Channel m_channel;

  /**
 * @param producer
 * @param ecInfo
 * @param maxNettyBackLog
 */
public EventConsumerActivationRequest(EventProducer producer, EventConsumerInfo ecInfo, long maxNettyBackLog, Channel channel) {
    m_producer = producer;
    m_ecInfo = ecInfo;
    m_maxNettybackLog = maxNettyBackLog;
    m_channel = channel;
  }

  /* (non-Javadoc)
 * @see com.ebay.jetstream.util.Request#execute()
 */
@Override
  public boolean execute() {

    try {
      m_producer.activateEventConsumer(m_ecInfo);
    }
    catch (Throwable t) {
      String msg = "Caught exception when executing EventConsumerActivationRequest - " + t.getMessage();

      LOGGER.warn( msg);
    }
    return true;
  }


@Override
public void operationComplete(ChannelFuture future) throws Exception {
	
	if (!future.isDone()) return;
	
	if (future.isSuccess()) {
		ConsumerChannelContext ccc = new ConsumerChannelContext();
	    ccc.setChannel(m_channel);
	    m_ecInfo.getVirtualQueueMonitor().setMaxQueueBackLog(m_maxNettybackLog);
	    ccc.setVirtualQueueMonitor(m_ecInfo.getVirtualQueueMonitor()); // one virtual queue monitor shared between all connections to one consumer
	    m_ecInfo.setChannelContext(m_channel, ccc);
	    m_producer.handleNewActiveConsumer(this);
	}
	else {
	      m_producer.markConsumerDead(m_ecInfo);
	}
	
}

}
