/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.scattergather;

/**
 * TaskMasterFactory - A class that creates TaskMaster instances.
 * 
 * @author shmurthy@ebay.com
 * 
 */


/**
 * @author shmurthy (shmurthy@ebay.com)
 * 
 */


public class TaskMasterFactory {

  TaskExecutor m_executor;// new TaskExecutor();

  /**
   * @return
   */
  public TaskMaster create() {

    TaskMaster newTaskMaster = new TaskMaster();
    if (m_executor == null) {
      m_executor = new TaskExecutor();
      m_executor.init();
    }
    newTaskMaster.setExecutor(m_executor);

    return newTaskMaster;

  }

  /**
   * @return
   */
  public TaskExecutor getExecutor() {
    return m_executor;
  }

  /**
   * @param executor
   */
  public void setExecutor(TaskExecutor executor) {
    m_executor = executor;
  }
}
