/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.scattergather;

/**
 * TaskExecutorRequest is a Request class that executes the specified Task
 * and notifies the specified TaskMaster once the execution is completed.
 * 
 * @author shmurthy@ebay.com
 * 
 */

import com.ebay.jetstream.util.Request;

/**
 * @author shmurthy (shmurthy@ebay.com)
 * 
 */


public class TaskExecutorRequest extends Request {

  private TaskMaster m_myMaster;
  private Task m_myTask;

  /**
   * @param master
   * @param task
   */
  public TaskExecutorRequest(TaskMaster master, Task task) {
    m_myMaster = master;
    m_myTask = task;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ebay.jetstream.util.Request#execute()
   */
  @Override
  public boolean execute() {

    try {
      m_myTask.execute();

    }
    catch (Throwable e) {
    }

    finally {

      m_myMaster.taskCompleted(m_myTask);
    }

    return false;
  }

  /**
   * @return
   */
  public TaskMaster getMyMaster() {
    return m_myMaster;
  }

  /**
   * @return
   */
  public Task getMyTask() {
    return m_myTask;
  }

  /**
   * @param myMaster
   */
  public void setMyMaster(TaskMaster myMaster) {
    m_myMaster = myMaster;
  }

  /**
   * @param myTask
   */
  public void setMyTask(Task myTask) {
    m_myTask = myTask;
  }

}
