/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support.channel;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

import com.ebay.jetstream.config.AbstractNamedBean;

public class PipelineFlowControl extends AbstractNamedBean implements ApplicationEventPublisherAware {
  private ApplicationEventPublisher m_publisher;
  private final Object m_source;

  public PipelineFlowControl(Object source) {
    m_source = source;
  }

  protected PipelineFlowControl() {
    m_source = this;
  }

  public Object getSource() {
    return m_source;
  }

  public void pause() {
    m_publisher.publishEvent(new PauseEvent(m_source));
  }

  public void resume() {
    m_publisher.publishEvent(new ResumeEvent(m_source));
  }

  public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
    m_publisher = applicationEventPublisher;
  }

  protected ApplicationEventPublisher getApplicationEventPublisher() {
    return m_publisher;
  }

}
