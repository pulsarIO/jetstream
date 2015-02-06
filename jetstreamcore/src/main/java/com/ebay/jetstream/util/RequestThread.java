/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a request thread pattern
 * 
 * *
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

public class RequestThread extends Thread {
  private FifoPriorityQueue m_queue = null;
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.util");
  private RequestThreadMonitor m_monitor;

  private final CountDownLatch endLatch = new CountDownLatch(1);

  public RequestThread() {
    super("RequestThread");
    setUncaughtExceptionHandler(new UncaughtRequestThreadExceptionHandler());
  }

  public RequestThread(FifoPriorityQueue queue) {
    super("RequestThread");
    setUncaughtExceptionHandler(new UncaughtRequestThreadExceptionHandler());
    m_queue = queue;
  }

  public RequestThread(FifoPriorityQueue queue, RequestThreadMonitor monitor) {
    super("RequestThread");
    setMonitor(monitor);

    setUncaughtExceptionHandler(new UncaughtRequestThreadExceptionHandler());
    m_queue = queue;
  }

  public RequestThreadMonitor getMonitor() {
    return m_monitor;
  }

  public void init(FifoPriorityQueue queue) {
    m_queue = queue;
    start();
  }

  @Override
  public void run() {

	  try {
		  while (true) {

			  try {

				  Request req = (Request) m_queue.removeHead();

				  if (req == null)
					  continue;

				  if (req instanceof AbortRequest)
					  return; // abort thread

				  if (m_monitor != null)
					  m_monitor.requestSubmitted();
				  req.execute();

				  req = null;
			  }
			  catch (Throwable t) {
				  LOGGER.error( "Exception \'" + t.getMessage() + "\' in thread " + getName());
			  }

			  finally {
				  if (m_monitor != null)
					  m_monitor.requestExecuted();
			  }

		  } // while loop
	  }
	  finally {
		  endLatch.countDown();
	  }
  }

  public void setMonitor(RequestThreadMonitor monitor) {
    m_monitor = monitor;
  }

  public void shutdown() {

    AbortRequest ar = new AbortRequest();
    m_queue.insertAtTail(ar);

    try {
	    endLatch.await();
    } catch (InterruptedException e) {
    	Thread.currentThread().interrupt();
    }
    // thread returns
  }

}
