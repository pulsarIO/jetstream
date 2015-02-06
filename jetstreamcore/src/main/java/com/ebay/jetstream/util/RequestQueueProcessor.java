/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.common.NameableThreadFactory;
import com.ebay.jetstream.xmlser.XSerializable;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;

/**
 * @author shmurthy
 *
 */

public class RequestQueueProcessor implements XSerializable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	private static class QueueProcessorExceptionHandler implements
			ExceptionHandler {

		AtomicLong m_dropCounter;
		String m_name;

		public QueueProcessorExceptionHandler(AtomicLong dropCounter,
				String name) {
			m_dropCounter = dropCounter;
			m_name = name;
		}

		@Override
		public void handleEventException(Throwable ex, long sequence,
				Object event) {

			try {
				m_dropCounter.incrementAndGet();
				LOGGER.error( "RequestQueueProcessor : " + m_name
						+ "  Caught Exception - sequence = " + sequence
						+ " Exception = " + ex.getLocalizedMessage());
			} catch (Throwable newEx) {
				// should never through exception on the exception handler.
				// ignore
			}

		}

		@Override
		public void handleOnStartException(Throwable ex) {

			LOGGER.error(
					"RequestQueueProcessor" + m_name
							+ " On Start exception - Exception = "
							+ ex.getLocalizedMessage());

		}

		@Override
		public void handleOnShutdownException(Throwable ex) {

			LOGGER.error(
					"RequestQueueProcessor" + m_name
							+ " On Shutdown exception - Exception = "
							+ ex.getLocalizedMessage());

		}

	}

	private static class RequestHolder<T> {
		private T item;

		public T remove() {
			T t = item;
			item = null;
			return t;
		}

		public void put(T event) {
			this.item = event;
		}
	}

	private static class RequestHolderFactory<T> implements
			EventFactory<RequestHolder<T>> {

		@Override
		public RequestHolder<T> newInstance() {
			return new RequestHolder<T>();
		}

	}

	private static class QueueProcessorWorkHandler implements
			WorkHandler<RequestHolder<Runnable>> {

		public QueueProcessorWorkHandler() {

		}

		@Override
		public void onEvent(RequestHolder<Runnable> event) throws Exception {

			Runnable req = event.remove();
		
			req.run();

		}
	}

	// worker pool related items

	private RequestHolderFactory<Runnable> m_eventFactory = new RequestHolderFactory<Runnable>();
	private WorkerPool m_worker;
	private RingBuffer m_ringBuffer;
	private SequenceBarrier m_barrier;
	private int m_numThreads = 1;
	private AtomicLong m_dropCounter = new AtomicLong(0);
	private int m_maxQueueSz = 30000;
	private ExecutorService m_executor;
	private String m_name;
	
	/**
	 * @param name
	 */
	public RequestQueueProcessor(String name) {
		this(30000, 1, name);
	}

	/**
	 * @param maxQueueSz
	 * @param numThreads
	 * @param name
	 */
	public RequestQueueProcessor(int maxQueueSz, int numThreads, String name) {

		m_name = name;
		m_maxQueueSz = maxQueueSz;

		m_ringBuffer = RingBuffer.createMultiProducer(m_eventFactory,
				normalizeBufferSize(m_maxQueueSz), new BlockingWaitStrategy());
		m_barrier = m_ringBuffer.newBarrier();

		m_numThreads = numThreads;

		QueueProcessorWorkHandler[] handlers = new QueueProcessorWorkHandler[m_numThreads];

		for (int i = 0; i < m_numThreads; i++) {
			handlers[i] = new QueueProcessorWorkHandler();
		}

		m_worker = new WorkerPool(m_ringBuffer, m_barrier,
				new QueueProcessorExceptionHandler(m_dropCounter, name),
				handlers);
		

		m_ringBuffer.addGatingSequences(m_worker.getWorkerSequences());
		
		m_executor = Executors.newFixedThreadPool(m_numThreads,
				new NameableThreadFactory(name));
		m_ringBuffer = m_worker.start(m_executor);

	}

	/**
	 * @param bufferSize
	 * @return
	 */
	private int normalizeBufferSize(int bufferSize) {
		if (bufferSize <= 0) {
			return 8192;
		}
		int ringBufferSize = 2;
		while (ringBufferSize < bufferSize) {
			ringBufferSize *= 2;
		}
		return ringBufferSize;
	}
	
	
	/**
	 * @param req
	 * @return
	 */
	public boolean processRequest(Runnable req) {
		long seq;

		try {
			seq = m_ringBuffer.tryNext();
		} catch (InsufficientCapacityException e1) {
			return false;
		}

		RequestHolder<Runnable> item = (RequestHolder<Runnable>) m_ringBuffer
				.get(seq);

		item.put(req);

		m_ringBuffer.publish(seq);

		return true;

	}

	/**
	 * @return
	 */
	public int getMaxQueueSz() {
		return m_maxQueueSz;
	}

	/**
	 * 
	 */
	public void shutdown() {
		if (m_worker.isRunning()) {
			m_worker.drainAndHalt();
			m_executor.shutdown();

			LOGGER.warn( "RequestQueueProcessor : " + m_name
				+ " Shutting down");
		}

	}

	/**
	 * @return
	 */
	public long getPendingRequests() {
		return m_ringBuffer.getBufferSize() - m_ringBuffer.remainingCapacity();
	}
	
	
	public boolean hasAvailableCapacity(int requiredCapacity) {
		return m_ringBuffer.hasAvailableCapacity(requiredCapacity);
	}

	/**
	 * @return
	 */
	public long getDroppedRequests() {
		return m_dropCounter.get();
	}

	/**
	 * @return
	 */
	public long getAvailableCapacity() {
		return m_ringBuffer.remainingCapacity();
	}

	/**
	 * Enable publish batch messages to the queue.
	 * 
	 * @param requests
	 * @return
	 */
	public boolean processBatch(List<Runnable> requests) {
		int batchSize = requests.size();
		long seq;

		try {
			seq = m_ringBuffer.tryNext(batchSize);
		} catch (InsufficientCapacityException e1) {

			return false;
		}

		for (int i = 0, t = batchSize; i < t; i++) {
			RequestHolder<Runnable> item = (RequestHolder<Runnable>) m_ringBuffer
					.get(seq - t + i + 1);

			item.put(requests.get(i));
		}

		if (batchSize > 1) {
			m_ringBuffer.publish(seq - batchSize + 1, seq);
		} else {
			m_ringBuffer.publish(seq);
		}

		return true;
	}

}
