/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.autoflush.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;



/**
 * @author shmurthy@ebay.com - Channel queue - Collection of MessageEvents*/

public class AutoFlushWriterChannelQueue {
	
	ConcurrentLinkedQueue<MessageEvent> m_queue = new ConcurrentLinkedQueue<MessageEvent>();
	private AtomicInteger m_bufferSize = new AtomicInteger(0);
	private AtomicInteger m_maxFlushBufferSz = new AtomicInteger(8192);
	private ChannelHandlerContext m_channelHandlerContext;
	private AtomicLong m_lastFlushTime = new AtomicLong(0);
	
	

	public long getLastFlushTime() {
		return m_lastFlushTime.get();
	}

	public void setLastFlushTime(long lastFlushTime) {
		this.m_lastFlushTime.set(lastFlushTime);
	}

	public ChannelHandlerContext getChannelHandlerContext() {
		return m_channelHandlerContext;
	}

	public void setChannelHandlerContext(
			ChannelHandlerContext channelHandlerContext) {
		this.m_channelHandlerContext = channelHandlerContext;
	}

	public AutoFlushWriterChannelQueue() {}
	
	public AutoFlushWriterChannelQueue(int maxFlushBufferSz) {
		m_maxFlushBufferSz.set(maxFlushBufferSz);
	}
		
		
	
	/**
	 * return true if time to flush else return false
	 * @param e
	 * @return 
	 */
	public void add(MessageEvent e) {
		
		m_queue.add(e);
	
		ByteBuf buf = (ByteBuf) e.getMsg();
		
		m_bufferSize.addAndGet(buf.readableBytes());
		
	}
	
	
	/**
	 * @return true if bufsize is >= max flush buf size else false
	 */
	public boolean isTimeToFlush() {
		if (m_bufferSize.get() >= m_maxFlushBufferSz.get()) {
			return true;
		}
		else
			return false;
	}
	
	
	
	public int size() {
		return m_queue.size();
	}
	
	/**
	 * @return
	 */
	
	public MessageEvent[] get() {
		
		if (m_queue.isEmpty()) return null;
		
		int size = m_queue.size();
			
		MessageEvent[] events = new MessageEvent[size];
		
		int bytesRead = 0;
		
		int polledEventCount = 0;
		
		for (; polledEventCount < size; polledEventCount++) {
			MessageEvent event = (MessageEvent) m_queue.poll();
			if (event != null) {
				events[polledEventCount] = event;
				bytesRead += ((ByteBuf) events[polledEventCount].getMsg()).readableBytes();
			}
			else
				break;
				
		}

		if (polledEventCount== 0) return null;
		
		m_bufferSize.addAndGet(-bytesRead);
		
		if (polledEventCount== size) {
			return events;
		}
		
		// we will pay a price of an extra copy only if some other thread took the event away from us after we got the size.
		
		MessageEvent[] actualEvents = new MessageEvent[polledEventCount];
		
		for (int x=0; x<polledEventCount; x++) {
			actualEvents[x] = events[x];
		}
		
		return actualEvents;
	}
	
		

	/**
	 * 
	 */
	public void clear() {
		m_queue.clear();

	}

	
	
}
