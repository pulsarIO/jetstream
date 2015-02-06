/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.autoflush.handler;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import com.ebay.jetstream.messaging.transport.netty.eventproducer.ExtendedChannelPromise;


/**
 * @author shmurthy@ebay.com - Future Listener installed by AutoFlushWriter
 */

public class AutoFlushWriterChannelListener implements ChannelFutureListener {


	MessageEvent[] m_events;

	public AutoFlushWriterChannelListener(MessageEvent[] events) {
		m_events = events;

	}



	@Override
	public void operationComplete(ChannelFuture future) throws Exception {

		if (future.isSuccess()) {
	        ExtendedChannelPromise bPromise = (ExtendedChannelPromise) future;
	        ExtendedChannelPromise firstEvent = (ExtendedChannelPromise) (m_events[0].getPromise());
	        firstEvent.setWrittenSize(bPromise.getWrittenSize());
	        firstEvent.setRawBytes(bPromise.getRawBytes());
	        firstEvent.setCompressedBytes(bPromise.getCompressedBytes());
			for (int i = 0; i < m_events.length; i++) {
				m_events[i].getPromise().setSuccess(null);

			}
		} else {
			Throwable cause = future.cause();
			for (int i = 0; i < m_events.length; i++) {
				m_events[i].getPromise().setFailure(cause);
			}
		}

	}

}
