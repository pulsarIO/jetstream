/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ebay.jetstream.util.FifoPriorityQueue.FifoPriorityQueueException;

public class RequestThreadTest {

  private static class RequestQueueMonitor implements QueueMonitor {

    private static class stats {
      private int m_pauseCount;
      private int m_resumeCount;

      /**
       * @return the pauseCount
       */
      public int getPauseCount() {
        return m_pauseCount;
      }

      /**
       * @return the resumeCount
       */
      public int getResumeCount() {
        return m_resumeCount;
      }

      /**
       * @param pauseCount
       *          the pauseCount to set
       */
      public void incPauseCount() {
        m_pauseCount++;
      }

      /**
       * @param resumeCount
       *          the resumeCount to set
       */
      public void incResumeCount() {
        m_resumeCount++;
      }

      public boolean pauseCntMatchesResumeCnt() {
        return m_pauseCount == m_resumeCount;
      }

    }

    private ArrayList<stats> m_mystatsList;
    private float m_highWaterMark = (float) 0.8;
    private float m_lowWaterMark = (float) 0.2;

    private RequestQueueMonitor() {
    }

    public RequestQueueMonitor(int numPrioritesToMonitor) {
      m_mystatsList = new ArrayList<stats>(numPrioritesToMonitor);

      for (int i = 0; i < numPrioritesToMonitor; i++) {
        m_mystatsList.add(new stats());
      }
    }

    public float getHighWaterMark() {
      return m_highWaterMark;
    }

    public float getLowWaterMark() {
      return m_lowWaterMark;
    }

    public boolean isPauseCntMatchesResumeCnt() {

      for (int i = 0; i < m_mystatsList.size(); i++) {
        if (!m_mystatsList.get(i).pauseCntMatchesResumeCnt())
          return false;
      }

      return true;
    }

    public void pause(long queueSize, int priority) {

      m_mystatsList.get(priority).incPauseCount();
      System.out.println("Received Pause : queue size = " + queueSize + " : priority = " + priority); //KEEPME

    }

    public void resume(long queueSize, int priority) {

      System.out.println("Received resume : queue size = " + queueSize + " : priority = " + priority); //KEEPME
      m_mystatsList.get(priority).incResumeCount();
    }

    public void setHighWaterMark(float highWaterMark) {
      m_highWaterMark = highWaterMark;
    }

    public void setLowWaterMark(float lowWaterMark) {
      m_lowWaterMark = lowWaterMark;
    }

  }

  private static class TestRequest extends Request {

    int m_delay = 0;

    public TestRequest(int delay) {
      m_delay = delay;
    }

    // @Override
    @Override
    public boolean execute() {

      try {
      
        m_execCount.addAndGet(1);
        if (m_delay > 0)
          Thread.sleep(m_delay);
      }
      catch (InterruptedException e) {
        
        e.printStackTrace(); //KEEPME
      }

      return false;
    }

  }

  static AtomicInteger m_execCount = new AtomicInteger(0);

  public static void main(String[] args) throws Exception {
    RequestThreadTest mytest = new RequestThreadTest();

    mytest.runtest(2, 50, 1000, 10, 2, true);
    /*
     * mytest.runtest(1, 50, 1000, 10, 1, true);
     *
     *
     * mytest.runtest(1, 50, 0, 10, 1, true);
     *
     *
     * mytest.runtest(2, 50, 0, 1000000, 1, true);
     *
     * mytest.runtest(1, 100000, 0, 1000000, 1, true);
     *
     * mytest.runtest(1, 100000, 0, 1000000, 1, false);
     *
     * mytest.runtest(2, 100000, 0, 1000000, 1, true);
     *
     * mytest.runtest(2, 100000, 0, 1000000, 3, true);
     *
     * mytest.runtest(2, 100000, 0, 1000000, 10, true);
     *
     *
     * mytest.runtest(2, 100000, 0, 1000000, 10, false);
     */
  }

  @Test
  public void executeTest() {

    RequestThreadTest mytest = new RequestThreadTest();

    mytest.runtest(2, 50, 1000, 10, 2, true);
    /*
     * mytest.runtest(1, 50, 1000, 10, 1, true);
     *
     * mytest.runtest(1, 50, 0, 10, 1, true);
     *
     * mytest.runtest(2, 50, 0, 1000000, 1, true);
     *
     * mytest.runtest(1, 100000, 0, 1000000, 1, true);
     *
     * mytest.runtest(1, 100000, 0, 1000000, 1, false);
     *
     * mytest.runtest(2, 100000, 0, 1000000, 1, true);
     *
     * mytest.runtest(2, 100000, 0, 1000000, 3, true);
     *
     * mytest.runtest(2, 100000, 0, 1000000, 10, true);
     *
     * mytest.runtest(2, 100000, 0, 1000000, 10, false);
     */
  }

  public void runtest(int numprios, int numreqs, int delay, int queueSize, int numThreads, boolean attachMonitor) {

    RequestThreadTest.m_execCount.set(0);

    FifoPriorityQueue myqueue = new FifoPriorityQueue(numprios);
    myqueue.setMaxSize(queueSize);

    // RequestThread mythread = new RequestThread(myqueue);
    RequestThreadPool mythreadpool = new RequestThreadPool();

    mythreadpool.init(myqueue, numThreads);

    RequestQueueMonitor myqueuemonitor = new RequestQueueMonitor(numprios);
    myqueuemonitor.setHighWaterMark((float) 0.85);
    myqueuemonitor.setLowWaterMark((float) 0.2);

    try {
      if (attachMonitor)
        myqueue.attachQueueMonitor(myqueuemonitor);
    }
    catch (FifoPriorityQueueException e1) {

      e1.printStackTrace();
      return;
    }

    try {
      mythreadpool.start();

    }
    catch (Exception e1) {

      e1.printStackTrace();
    }

    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException e2) {

      e2.printStackTrace();
    }

    Timer mytimer = new Timer();

    mytimer.start();

    long failedCnt = 0;

    for (int i = 0; i < numreqs; i++) {
      for (int j = 0; j < numprios; j++) {
        Request myrequest = new TestRequest(delay);
        myrequest.setPriority(j);

        if (!myqueue.insertAtTail(myrequest, j))
          failedCnt += 1;

      }
    }

    System.out.println("time taken to submit " + numreqs * numprios + " reqs = " + mytimer.elapsedTimeInMillis()); //KEEPME

    while (true) {
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      if (myqueue.isEmpty())
        break;
    }

    System.out.println("time taken to submit and execute " + numreqs * numprios + " reqs with " + numThreads
        + " with queue monitoring " + attachMonitor + " time = " + mytimer.elapsedTimeInMillis() + " millisecs"); //KEEPME

    mythreadpool.shutdown();

    System.out.println("Total requests executed = " + RequestThreadTest.m_execCount); //KEEPME
    System.out.println("My pause count matches Resume count = " + myqueuemonitor.isPauseCntMatchesResumeCnt()); //KEEPME
    System.out.println("Failed count = " + failedCnt); //KEEPME
    assertFalse(!myqueuemonitor.isPauseCntMatchesResumeCnt());
  }
}
