/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.requestthreadpattern;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.ebay.jetstream.util.FifoPriorityQueue;
import com.ebay.jetstream.util.FifoPriorityQueue.FifoPriorityQueueException;
import com.ebay.jetstream.util.QueueMonitor;
import com.ebay.jetstream.util.Request;
import com.ebay.jetstream.util.RequestThreadPool;

/**
 * @author shmurthy
 *
 */
public class RequestThreadTest implements QueueMonitor {

  public static class ThreadTestRequest extends Request {

    /*
     * (non-Javadoc)
     *
     * @see com.ebay.jetstream.util.Request#execute()
     */
    @Override
    public boolean execute() {

      try {
        Thread.sleep(1000);
        RequestThreadTest.m_requestProcessed.addAndGet(1);
      }
      catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      return true;

    }

  }

  public static AtomicLong m_requestProcessed = new AtomicLong(0);

  private static final int MAX_QUEUE_SIZE = 100;
  private static final int THREAD_POOL_SIZE = 10;

  public static void main(String[] args) throws Exception {

    RequestThreadTest rtt = new RequestThreadTest();

    rtt.runTest();

    System.out.println(".......Test Ended ......."); //KEEPME
  }

  private final static float m_lowWaterMark = 0.25f;
  private final static float m_highWaterMark = 0.85f;
  private int m_pauseCount = 0;
  private int m_resumeCount = 0;
  private FifoPriorityQueue m_eventQueue;

  private RequestThreadPool m_requestThreadPool;

  public RequestThreadTest() {
  }

  /*
   * (non-Javadoc)
   *
   * @see com.ebay.jetstream.util.QueueMonitor#getHighWaterMark()
   */
  public float getHighWaterMark() {

    return m_highWaterMark;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.ebay.jetstream.util.QueueMonitor#getLowWaterMark()
   */
  public float getLowWaterMark() {

    return m_lowWaterMark;
  }

  public int getPauseCount() {
    return m_pauseCount;
  }

  public int getResumeCount() {
    return m_resumeCount;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.ebay.jetstream.util.QueueMonitor#pause(long, int)
   */
  public void pause(long queueSize, int priority) {

    System.out.println("recvd pause");

    m_pauseCount += 1;

  }

  /*
   * (non-Javadoc)
   *
   * @see com.ebay.jetstream.util.QueueMonitor#resume(long, int)
   */
  public void resume(long queueSize, int priority) {

    System.out.println("recvd resume"); //KEEPME

    m_resumeCount += 1;

  }

  @Test
  public void runTest() {
    m_eventQueue = new FifoPriorityQueue();
    m_eventQueue.setMaxSize(MAX_QUEUE_SIZE);

    try {
      m_eventQueue.attachQueueMonitor(this);
    }
    catch (FifoPriorityQueueException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    m_requestThreadPool = new RequestThreadPool();

    m_requestThreadPool.init(m_eventQueue, THREAD_POOL_SIZE);

    try {
      m_requestThreadPool.start();
    }
    catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    for (int i = 0; i < MAX_QUEUE_SIZE; i++) {

      ThreadTestRequest ttr = new ThreadTestRequest();
      m_eventQueue.insertAtTail(ttr);

    }

    while (m_eventQueue.size(0) > 0) {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } 
    
    
    System.out.println("Pause Count = " + m_pauseCount + " : Resume Count = " + m_resumeCount); //KEEPME
    System.out.println("Requests Processed = " + RequestThreadTest.m_requestProcessed.get());//KEEPME

    assertEquals(true, m_pauseCount == m_resumeCount);
    RequestThreadTest.m_requestProcessed.get();

    m_requestThreadPool.shutdown();

    System.out.println("Test passed Successfully."); //KEEPME

  }

  public void setPauseCount(int pauseCount) {
    m_pauseCount = pauseCount;
  }

  public void setResumeCount(int resumeCount) {
    m_resumeCount = resumeCount;
  }

}
