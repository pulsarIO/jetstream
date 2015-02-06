/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ebay.jetstream.xmlser.XSerializable;
// import java.util.Vector;

/**
 * An implementation of a queue of request messages. If a QueueMonitor is attached to this queue, it can monitor
 * crossing of the high and low water mark thresholds and notify the queue monitor when thresholds are crossed
 * 
 * *
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

/*

package com.ebay.jetstream.util;

import java.util.ArrayList; // import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ebay.jetstream.xmlser.XSerializable;

/**
 * An implementation of a queue of request messages. If a QueueMonitor is attached to this queue, it can monitor
 * crossing of the high and low water mark thresholds and notify the queue monitor when thresholds are crossed
 * 
 * *
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC")
public class FifoPriorityQueue implements XSerializable {

  public static class FifoPriorityQueueException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    public final static int DATA_OUT_OF_RANGE_ERROR = 1;
    public final static int HIGHWATERMARK_LESS_THAN_LOWWATERMARK = 2;

    private int m_error = 0;
    private String m_message;

    /**
     * @param error
     * @param message
     */
    public FifoPriorityQueueException(int error, String message) {

      m_error = error;
      m_message = message;
    }

    /**
     * @return the error
     */
    public int getError() {
      return m_error;
    }

    /**
     * @param error
     *          the error to set
     */
    public void setError(int error) {
      m_error = error;
    }

    /**
     * @return the message
     */
    public String getMessage() {
      return m_message;
    }

    /**
     * @param message
     *          the message to set
     */
    public void setMessage(String message) {
      m_message = message;
    }

    public boolean isDataOutOfRange() {
      return (m_error == DATA_OUT_OF_RANGE_ERROR);
    }

    public boolean isHighWaterMarkLessThanLowWaterMark() {
      return (m_error == HIGHWATERMARK_LESS_THAN_LOWWATERMARK);
    }

  }

  public final static int HIGH_PRIORITY = 0;
  public final static int LOW_PRIORITY = 1;

  private float m_highWaterMark = (float) 0.85; // percentage of queue size
  private float m_lowWaterMark = (float) 0.25; // percentage of queue size
  private QueueMonitor m_queueMonitor = null;
  private ArrayList<AtomicBoolean> m_pausedList; // each entry corresponds to a priority queue
  private volatile boolean m_waitersWaiting = false;

  /**
   * An array of Vectors, each for a message priority The default constructor assumes only one entry.
   */

  private ArrayList<ConcurrentLinkedQueue<Object>> m_requestList;
  /**
   * Number of priority types.
   */
  private int m_numPriority;
  /**
   * Maximum allowable size of the queues. A value of -1 indicates no limit.
   */
  private volatile int m_maxSize = -1;
  /**
   * Total number of messages comming in to this queue
   */
  private long m_msgIn = 0;
  /**
   * Total number of messages going out of this queue
   */
  private long m_msgOut = 0;

  /**
   * Default constructor. Uses only 1 priority type.
   */
  public FifoPriorityQueue() {
    m_requestList = new ArrayList<ConcurrentLinkedQueue<Object>>(1);
    m_requestList.add(0, new ConcurrentLinkedQueue<Object>());
    m_numPriority = 1;

    createPausedList(1);

  }

  /**
   * Alternative constructor. Implements multiple priority queues.
   * 
   * @param numPriority
   *          Number of priority types
   */
  public FifoPriorityQueue(int numPriority) {
    if (numPriority <= 0)
      numPriority = 1;
    m_numPriority = numPriority;

    m_requestList = new ArrayList<ConcurrentLinkedQueue<Object>>(m_numPriority);

    for (int i = 0; i < numPriority; i++)
      m_requestList.add(i, new ConcurrentLinkedQueue<Object>());

    createPausedList(numPriority);
  }

  /**
   * Sets the maximum size of the request queue
   * 
   * @param maxSize
   *          Desired maximum size of the queue
   */
  public void setMaxSize(int maxSize) {
    m_maxSize = maxSize;
  }

  /**
   * Gets the current maximum size of the request queue
   * 
   * @return Current maximum queue size limit.
   */
  public int getMaxSize() {
    return m_maxSize;
  }

  /**
   * Gets the current size of the request queue.
   * 
   * @param prio
   *          The queue to get size for.
   * @return Current size.
   */
  public int size(int priority) {
    if ((priority < 0) || (priority >= m_numPriority))
      return -1;
    return m_requestList.get(priority).size();
  }

  /**
   * Get the total number of incomming messages handled
   * 
   * @return Number of messages
   */
  public long getMsgIn() {
    return m_msgIn;
  }

  /**
   * Get the total number of outgoing messages handled
   * 
   * @return Number of messages
   */
  public long getMsgOut() {
    return m_msgOut;
  }

  /**
   * Fetches the first request from the highest priority queue.
   * 
   * @return The first message from the highest priority queue.
   */
 
  public Object removeHead() {
	  Object obj = null;
	  // Find if any queue has a message pending, in order of
	  // highest to lowest priority.
	  int prio; // Highest priority queue with a message.
	  do {
		  for (prio = 0; prio < m_numPriority; prio++) {
			  evaluateLowWaterMarkCrossing(prio);
			  if (!m_requestList.get(prio).isEmpty())
				  break;
		  }
		  if (prio != m_numPriority)
			  break;
		  // No pending messages.
		  synchronized(this) {
			  try {
				  m_waitersWaiting = true;
                  for (prio = 0; prio < m_numPriority; prio++) {
                      evaluateLowWaterMarkCrossing(prio);
                      if (!m_requestList.get(prio).isEmpty())
                          break;
                  }
                  if (prio != m_numPriority)
                      break;
				  wait();
			  }
			  catch (InterruptedException ie) {
			  }
		  }
	  } while (true);
	  try {

		  obj = m_requestList.get(prio).poll();
	  }
	  catch (java.util.NoSuchElementException e) {

	  }

	  m_msgOut++;
	  return obj;
  }

  /**
   * Insert a message into the highest priority queue.
   * 
   * @param req
   *          Request to be inserted.
   */
 
  public boolean insertAtTail(Object obj) {
    return insertAtTail(obj, 0);
  }

  /**
   * Insert a message into the supplied priority queue.
   * 
   * @param req
   *          Request to be inserted.
   * @param prio
   *          Priority of the queue to insert into
   * @return true, if successful.
   */
  
  public boolean insertAtTail(Object obj, int prio) {
	  evaluateHighWaterMarkCrossing(prio);

	  if (prio < 0 || prio >= m_numPriority)

		  return false;

	  int size = m_requestList.get(prio).size();

	  if (m_maxSize > 0 && size >= m_maxSize)

		  return false;

	  if (!m_pausedList.get(prio).get()) {

		  m_requestList.get(prio).offer(obj);

		  m_msgIn++;

		  if (m_waitersWaiting) {
			  synchronized(this) {
			      m_waitersWaiting = false;
				  notifyAll();
			  }
		  }
	  }

	  return true;

  }

  /**
   * @return the numPriority
   */
  public int getNumPriority() {
    return m_numPriority;
  }

  /**
   * @return
   */
  public boolean isEmpty() {
    for (int i = 0; i < m_numPriority; i++) {
      if (!m_requestList.get(i).isEmpty())
        return false;
    }

    return true;
  }

  /**
   * @return the lowWaterMark
   */
  public float getLowWaterMark() {
    return m_lowWaterMark;
  }

  public float getHighWaterMark() {
    return m_highWaterMark;
  }

  /**
   * @return the queueMonitor
   */
  public QueueMonitor getQueueMonitor() {
    return m_queueMonitor;
  }

  /**
   * @param attachQueueMonitor
   *          - any old attachments will be overwritten
   * @param highWaterMark
   *          the highWaterMark to set as a percentage of max queue size
   * @param lowWaterMark
   *          the lowWaterMark to set as a percentage of max queue size
   */
  public void attachQueueMonitor(QueueMonitor queueMonitor) throws FifoPriorityQueueException {

    if ((queueMonitor.getLowWaterMark() < 0.0) || (queueMonitor.getLowWaterMark() > 1.0))
      throw new FifoPriorityQueueException(FifoPriorityQueueException.DATA_OUT_OF_RANGE_ERROR,
          "low water mark out of range - must be between 0.0 and 1.0");

    if ((queueMonitor.getHighWaterMark() < 0.0) || (queueMonitor.getHighWaterMark() > 1.0))
      throw new FifoPriorityQueueException(FifoPriorityQueueException.DATA_OUT_OF_RANGE_ERROR,
          "low water mark out of range - must be between 0.0 and 1.0");

    if (queueMonitor.getHighWaterMark() < queueMonitor.getLowWaterMark())
      throw new FifoPriorityQueueException(FifoPriorityQueueException.HIGHWATERMARK_LESS_THAN_LOWWATERMARK,
          "low water mark greater than high water mark");

    m_highWaterMark = queueMonitor.getHighWaterMark();
    m_lowWaterMark = queueMonitor.getLowWaterMark();
    m_queueMonitor = queueMonitor;

  }

  /**
   * 
   */
  public void detachQueueMonitor() {
    m_queueMonitor = null;
  }

  /**
   * @param size
   */
  private void createPausedList(int size) {
    m_pausedList = new ArrayList<AtomicBoolean>(size);
    for (int i = 0; i < size; i++) {
      m_pausedList.add(i, new AtomicBoolean(false));
    }
  }

  /**
   * @param prio
   */
  private void evaluateHighWaterMarkCrossing(int prio) {

    if (m_queueMonitor == null)
      return;

    AtomicBoolean isPaused = m_pausedList.get(prio);

    if (!isPaused.get()) {
      if (m_requestList.get(prio).size() >= (int) (m_highWaterMark * getMaxSize())) {
        m_queueMonitor.pause(m_requestList.get(prio).size(), prio);
        isPaused.set(true);
      }
    }
  }

  /**
   * @param prio
   */
  private void evaluateLowWaterMarkCrossing(int prio) {

	  if (m_queueMonitor == null)
		  return;

	  AtomicBoolean isPaused = m_pausedList.get(prio);

	
	  if (isPaused.get()) {
		  if (m_requestList.get(prio).size() <= (int) (m_lowWaterMark * getMaxSize())) {

			  if (isPaused.compareAndSet(true, false))
				  m_queueMonitor.resume(m_requestList.get(prio).size(), prio);
	
		  }
	  }
  }
}


