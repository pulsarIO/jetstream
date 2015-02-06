/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.scattergather;

/**
 * TaskExecutor is a space where scatter gather tasks are executed.
 * It holds a FifoQueue and a pool of threads.
 * 
 * @author shmurthy@ebay.com
 * 
 */



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.util.FifoPriorityQueue;
import com.ebay.jetstream.util.RequestThreadMonitor;
import com.ebay.jetstream.util.RequestThreadPool;
import com.ebay.jetstream.xmlser.XSerializable;

public class TaskExecutor implements XSerializable, InitializingBean {

  private RequestThreadPool m_taskExecutorThreadPool;
  private FifoPriorityQueue m_taskQueue;
  private int m_threadPoolSize = 60;
  private int m_queueSize = m_threadPoolSize + 1;
  Logger logger = LoggerFactory.getLogger("com.ebay.jetstream.scattergather.TaskExecutor");
	
  
  
  public TaskExecutor() {
    Management.addBean("TaskMaster/TaskExecutor", this);
  }
  
  public void init() {
    
    if (m_taskQueue == null) {
      m_taskQueue = new FifoPriorityQueue();
      m_taskQueue.setMaxSize(m_threadPoolSize + 1);
    }

    if (m_taskExecutorThreadPool == null) {
      m_taskExecutorThreadPool = new RequestThreadPool();
      m_taskExecutorThreadPool.setMonitor(new RequestThreadMonitor());
      m_taskExecutorThreadPool.init(m_taskQueue, m_threadPoolSize);
      try {
        m_taskExecutorThreadPool.start();
      }
      catch (Exception e) {
        logger.error( "EXCEPTION when starting RequestThreadpool from TaskExecutor " + e.getLocalizedMessage(), e); 
            
      }
    }
  }

  /**
   * @param ter
   * @throws Exception
   */
  public void execute(TaskExecutorRequest ter) throws Exception {
    m_taskQueue.insertAtTail(ter);
  }

  public int getCurrentQueueSize() {
    return m_taskQueue.size(0);
  }

  /**
   * @return the queueSize
   */
  public int getQueueSize() {
    return m_queueSize;
  }

  /**
   * @return
   */
  public int getThreadPoolSize() {
    return m_threadPoolSize;
  }

  /**
   * @param queueSize
   *          the queueSize to set
   */
  public void setQueueSize(int queueSize) {
    m_queueSize = queueSize;
  }

  /**
   * @param threadPoolSize
   */
  public void setThreadPoolSize(int threadPoolSize) {
    m_threadPoolSize = threadPoolSize;
    m_queueSize = m_threadPoolSize + 1 ; 
  }
  
  public long getActiveThreadCount(){
    if(m_taskExecutorThreadPool.getMonitor() != null)
      return m_taskExecutorThreadPool.getMonitor().getActiveThreadCount();
    
    // no monitor is set to give the active thread count. So returning 0 will not be correct. Returning -1.
    return -1; 
  }

  public void afterPropertiesSet() throws Exception {
    init();
  }

}
