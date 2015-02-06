/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of a request thread pattern
 * 
 * *
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

public class RequestThreadPool {
  FifoPriorityQueue m_queue = null;
  AtomicInteger m_numThreads = new AtomicInteger(2);
  CopyOnWriteArrayList<RequestThread> m_threadList;
  boolean m_initialized = false;
  private RequestThreadMonitor m_monitor;
  
  public int getNumThreads() {
	return m_numThreads.get();
  }

    public RequestThreadPool() {
  }

  public RequestThreadMonitor getMonitor() {
    return m_monitor;
  }

  public void init(FifoPriorityQueue queue, int numThreads) {
    m_queue = queue;
    m_numThreads.set(numThreads);
    m_threadList = new CopyOnWriteArrayList<RequestThread>();
    m_initialized = true;
  }

  public void setMonitor(RequestThreadMonitor monitor) {
    m_monitor = monitor;
  }

  public synchronized void shutdown() {

    for (int i = 0; i < m_numThreads.get(); i++) {

      AbortRequest ar = new AbortRequest();

      m_queue.insertAtTail(ar);
    }

  }

  public synchronized void start() throws Exception {
    if (!m_initialized)
      throw new Exception("Thread pool not initialized");

    for (int i = 0; i < m_numThreads.get(); i++) {
      if (m_monitor != null)
        m_threadList.add(i, new RequestThread(m_queue, m_monitor));
      else
        m_threadList.add(i, new RequestThread(m_queue));

      m_threadList.get(i).start();
    }
  }
  
	public void increasePoolSize(int numThreads) {
		for (int i = 0; i < numThreads; i++) {
			if (m_monitor != null)
				m_threadList.add(m_numThreads.get() + i, new RequestThread(
						m_queue, m_monitor));
			else
				m_threadList.add(m_numThreads.get() + i, new RequestThread(
						m_queue));

			m_threadList.get(m_numThreads.get() + i).start();
			
			m_numThreads.addAndGet(numThreads);
		}
	}

	public void decreasePoolSize(int numThreads) {
		for (int i = 0; i < numThreads; i++) {
			 AbortRequest ar = new AbortRequest();

		      m_queue.insertAtTail(ar);
		}
	}

}
