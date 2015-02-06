/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.autoflush.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.messaging.transport.netty.eventproducer.ExtendedChannelPromise;
import com.ebay.jetstream.xmlser.XSerializable;


/**
 * @author shmurthy@ebay.com - This is a batcher which batches events to write larger payloads to socket. It flushes the buffer
 * 			                   when it collects provisioned number of bytes or the timer fires.
 */


@Sharable
public class NettyAutoFlushBatcher extends ChannelDuplexHandler implements XSerializable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private final int LAST_CHANNEL_FLUSH_INTERVAL=500;
	private FlushTimer m_timer;
	private int m_flushTimerIntervalInMillis = 500;
	private AtomicInteger m_maxFlushBufferSz = new AtomicInteger(8192);
	private ConcurrentHashMap<Channel, AutoFlushWriterChannelQueue> m_channelQueue = new ConcurrentHashMap<Channel, AutoFlushWriterChannelQueue>();

	
	
	/**
	 * @return
	 */
	public int getMaxFlushBufferSz() {
		return m_maxFlushBufferSz.get();
	}

	
	
	/**
	 * @param maxFlushBufferSz
	 */
	public void setMaxFlushBufferSz(int maxFlushBufferSz) {
		this.m_maxFlushBufferSz.set(maxFlushBufferSz);
	}

	
	
	/**
	 * 
	 */
	public NettyAutoFlushBatcher() {
		m_timer = new FlushTimer(this, m_flushTimerIntervalInMillis);

	}

	
	
	
	/**
	 * @param autoFlushSz
	 * @param flushTimerIntervalInMillis
	 */
	public NettyAutoFlushBatcher(int autoFlushSz,
			int flushTimerIntervalInMillis ) {

		m_maxFlushBufferSz.set(autoFlushSz);
		m_flushTimerIntervalInMillis = flushTimerIntervalInMillis;
		m_timer = new FlushTimer(this, m_flushTimerIntervalInMillis);

	}

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelDuplexHandler#write(io.netty.channel.ChannelHandlerContext, java.lang.Object, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception 
	{

		if (!ctx.channel().isActive()) {
		    
		    ReferenceCountUtil.release(msg);

			// we might get here as channel is closed but upstream has not been notified yet. It could be still sending us events.
			// in such a case we will inform the future and send an exception upstream

			Throwable cause = new Exception("passed channel not active - " + ((InetSocketAddress) ctx.channel().remoteAddress())
					.getAddress().getHostAddress());
						
			promise.setFailure(cause);

			ctx.fireExceptionCaught(cause);

			return;
		}

		AutoFlushWriterChannelQueue queue = m_channelQueue.get(ctx.channel());

		if (queue == null) {
			queue = new AutoFlushWriterChannelQueue(m_maxFlushBufferSz.get());
			queue.setChannelHandlerContext(ctx);
			m_channelQueue.put(ctx.channel(), queue);
		}

		MessageEvent e = new MessageEvent(msg, promise);

		queue.add(e);


		if (queue.isTimeToFlush()) {

			flush(ctx, queue);

		}

	}

	
	
	
	/**
	 * @param ctx
	 * @param queue
	 */
	public void flush(ChannelHandlerContext ctx,
			AutoFlushWriterChannelQueue queue) {

		if (queue == null)
			return;

		MessageEvent[] events = queue.get();

		if (events == null)
			return;

		ByteBuf[] msgs = new ByteBuf[events.length];

		for (int j=0; j < events.length; j++) {
			msgs[j] = (ByteBuf) events[j].getMsg();
		}


		try {
			ByteBuf composite = Unpooled.wrappedBuffer(msgs);

			ChannelPromise promise = new ExtendedChannelPromise(ctx.channel());

			promise.addListener(new AutoFlushWriterChannelListener(events));

			queue.setLastFlushTime(System.currentTimeMillis());

			super.write(ctx, composite, promise);
			super.flush(ctx);

		} catch (Throwable t) {
			LOGGER.error( "Error while Flushing : " + Arrays.toString(t.getStackTrace()));
		}

	}

	/**
	 * 
	 */
	public void shutdown() {

		LOGGER.info( "Shutting down AutoFlushBufferWriter");

		if (m_timer != null)
			m_timer.cancel();

		flush();

		m_channelQueue.clear();
	}

	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelDuplexHandler#connect(io.netty.channel.ChannelHandlerContext, java.net.SocketAddress, java.net.SocketAddress, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
			SocketAddress localAddress, ChannelPromise promise) throws Exception {
		ctx.connect(remoteAddress, localAddress, promise);

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
		try {
			flush(ctx, m_channelQueue.get(ctx.channel()));
			m_channelQueue.remove(ctx.channel());
		} finally {
			ctx.disconnect(promise);
		}

	}


	/* (non-Javadoc)
	 * @see io.netty.channel.ChannelDuplexHandler#close(io.netty.channel.ChannelHandlerContext, io.netty.channel.ChannelPromise)
	 */
	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {


		AutoFlushWriterChannelQueue queue = m_channelQueue.remove(ctx.channel());

		if (queue == null) {

			return;
		}

		MessageEvent[] events = queue.get();

		if (events == null) {
			ctx.close();
			return;
		}

		Throwable cause = new ClosedChannelException();

		for (int i=0; i < events.length; i++) {

			MessageEvent ev = events[i];

			ReferenceCountUtil.release(ev.getMsg());
			
			ev.getPromise().setFailure(cause);

		}

		super.close(ctx, promise);


	}



	/**
	 * 
	 */
	 public void flush() {

		 if (m_channelQueue.isEmpty()) return;

		 Set<Entry<Channel, AutoFlushWriterChannelQueue>> channels = m_channelQueue.entrySet();

		 if (channels == null) return; // defensive programming

		 Iterator<Entry<Channel, AutoFlushWriterChannelQueue>> itr = channels.iterator();

		 long t1 = System.currentTimeMillis();

		 while(itr.hasNext()) {

			 Entry<Channel, AutoFlushWriterChannelQueue> entry = itr.next();

			 AutoFlushWriterChannelQueue queue = entry.getValue();

			 if (queue.size() == 0) continue;

			 long elapsedTime = System.currentTimeMillis() - queue.getLastFlushTime();

			 if (elapsedTime > LAST_CHANNEL_FLUSH_INTERVAL) {
				 // now it is time to flush


				 if (LOGGER.isDebugEnabled())
					 LOGGER.debug( entry.getKey().toString() + "time to flush - queue size = " + queue.size() + " elapsedTime = " + elapsedTime);

				 flush(queue.getChannelHandlerContext(), queue);
			 }

		 }

		 long t2 = System.currentTimeMillis();



		 if (LOGGER.isDebugEnabled())
			 LOGGER.debug( "flush processing time = " + (t2-t1));
	 }



	 /* (non-Javadoc)
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#channelInactive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	 public void channelInactive(ChannelHandlerContext ctx) throws Exception {

		 AutoFlushWriterChannelQueue queue = m_channelQueue.remove(ctx.channel());

		 if (queue == null) {
			 ctx.fireChannelInactive();
			 return;
		 }

		 MessageEvent[] events = queue.get();

		 if (events == null) {
			 ctx.fireChannelInactive();
			 return;
		 }

		 Throwable cause = new ClosedChannelException();


		 for (int i=0; i < events.length; i++) {

			 MessageEvent ev = events[i];

			 Promise promise = ev.getPromise();

			 if (promise != null)
				 promise.setFailure(cause);

			 ((ByteBuf) ev.getMsg()).release();

		 }


		 if (queue != null) {
			 queue.clear();

		 }

		 ctx.fireChannelInactive();
	 }
	 
	 
	 
	 
	 /* (non-Javadoc)
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
	 */
	@Override
	 public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			 throws Exception {

		 LOGGER.error( cause.getLocalizedMessage(), cause);

		 AutoFlushWriterChannelQueue queue = m_channelQueue.remove(ctx.channel());

		 if (queue == null) return;

		 MessageEvent[] events = queue.get();

		 if (events == null) return;

		 for (int i=0; i < events.length; i++) {

			 MessageEvent ev = events[i];

			 Promise promise = ev.getPromise();

			 if (promise != null)
				 promise.setFailure(cause);

			 ((ByteBuf) ev.getMsg()).release();

		 }


		 if (queue != null) {
			 queue.clear();

		 }

		 super.exceptionCaught(ctx, cause);
	 }

	 
}
