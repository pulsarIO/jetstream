/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.handler.codec.http.HttpResponse;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public abstract class AbstractResponseFuture implements ResponseFuture {

  private final AtomicBoolean m_success = new AtomicBoolean(false);
  private final AtomicBoolean m_failure = new AtomicBoolean(false);
  private final AtomicBoolean m_timedout = new AtomicBoolean(false);

  public boolean isFailure() {
    return m_failure.get();
  }

  public boolean isSuccess() {
    return m_success.get();
  }

  public boolean isTimedout() {
    return m_timedout.get();
  }

  @Override
  public abstract void operationComplete(HttpResponse response);

  @Override
  public void setFailure() {
    m_failure.set(true);

  }

  @Override
  public void setSuccess() {
    m_success.set(true);

  }

  @Override
  public void setTimedout() {
    m_timedout.set(true);

  }

}
