/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.scattergather;

/**
 * TaskMaster is created by the TaskMasterFactory. It provides an interface
 * for user to submit scatter gather tasks and get them executed. Upon 
 * successful execution of the tasks the successfully executed tasks are
 * returned back to the caller. It allows a caller to specify the timeout 
 * duration that it must wait for completing the task.
 * 
 * @author shmurthy@ebay.com
 * 
 */


/**
 * @author shmurthy (shmurthy@ebay.com)
 * 
 */


import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskMaster {

  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.scattergather");
  private final LinkedList<Task> m_submittedTaskList = new LinkedList<Task>();
  private final LinkedList<Task> m_executedTaskList = new LinkedList<Task>();
  private TaskExecutor m_executor;
  private final Object m_taskCompletionMonitor = new Object();
  private long m_maxWaitTime = 1000;
  private final AtomicBoolean m_abandonTask = new AtomicBoolean(false);
  
  public void abandonTask() {
    m_abandonTask.set(true);
  }

  /**
   * @param timeout
   * @return
   */
  public LinkedList<Task> executeTasks(long timeout) {

    synchronized (m_submittedTaskList) {

      Iterator<Task> itr = m_submittedTaskList.iterator();

      while (itr.hasNext()) {

        Task t = itr.next();

        t.setAbandonStatusIndicator(m_abandonTask);

        TaskExecutorRequest ter = new TaskExecutorRequest(this, t);

        try {
          m_executor.execute(ter);
        }
        catch (Exception e) {
          // TODO see how to handle this - if we are here it means we have no more resources left
          // problem is part of the task might be already submitted. What do we do with that
          // we might want to throw an exception at this point or continue with the submitted list
        	
          LOGGER.error( "Failed to execute task due to lack of resources - " + e.getLocalizedMessage());
        }

      }

    }

    long expiryTime = System.currentTimeMillis() + timeout;
    long remainingTime = timeout;
    boolean noTaskExecuted = true;
    boolean maxTimeoutExpired = false;

    while (true) {
      synchronized (m_taskCompletionMonitor) {
        try {
          m_taskCompletionMonitor.wait(remainingTime);
        }
        catch (InterruptedException ie) {
        }
      }
      if (m_executedTaskList.size() > 0) {
        noTaskExecuted = false;
      }
      if (m_submittedTaskList.size() == m_executedTaskList.size())
        break;
      
      remainingTime = expiryTime - System.currentTimeMillis();
      if (maxTimeoutExpired) {
        abandonTask();
        break;
      }
      if (remainingTime <= 0) {
        if (noTaskExecuted) {
          LOGGER.warn( "Timeout Extended by 1 SECOND");
          remainingTime = getMaxWaitTime(); // wait a max of 1 more sec
          maxTimeoutExpired = true;
        }
        else {
          abandonTask();
          break;
        }
      }
    }

    return m_submittedTaskList;
  }

  /**
   * @return the executedTaskList
   */
  public LinkedList<Task> getExecutedTaskList() {
    return m_executedTaskList;
  }

  /**
   * @return
   */
  public TaskExecutor getExecutor() {
    return m_executor;
  }

  public long getMaxWaitTime() {
    return m_maxWaitTime;
  }

  /**
   * @return the submittedTaskList
   */
  public LinkedList<Task> getSubmittedTaskList() {
    return m_submittedTaskList;
  }

  /**
   * @param executor
   */
  public void setExecutor(TaskExecutor executor) {
    m_executor = executor;
  }

  public void setMaxWaitTime(long maxWaitTime) {
    m_maxWaitTime = maxWaitTime;
  }

  /**
   * @param task
   */
  public void submitTask(Task task) {
    synchronized (m_submittedTaskList) {
      m_submittedTaskList.add(task);
    }

  }

  /**
   * @param task
   */
  public void taskCompleted(Task task) {

    synchronized (m_executedTaskList) {
      task.executed();
      m_executedTaskList.add(task);
    }

    synchronized (m_taskCompletionMonitor) {

      m_taskCompletionMonitor.notifyAll();

    }

  }

}
