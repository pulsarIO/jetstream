/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support.channel;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.event.channel.InboundChannel;
import com.ebay.jetstream.management.Management;

@ManagedResource(objectName = "Event", description = "Inbound message flow control publisher")
public class ChannelFlowManagement extends PipelineFlowControl implements ApplicationListener {

  // At each refresh and bean change event, if there are any top-level inbound channels, register this
  // as a management bean to support pause/resume of them
  public void onApplicationEvent(ApplicationEvent event) {
    if (event instanceof ContextRefreshedEvent || event instanceof ContextBeanChangedEvent) {
      ApplicationContext context = ((ApplicationContextEvent) event).getApplicationContext();
      boolean hasInboundChannels = !context.getBeansOfType(InboundChannel.class).isEmpty();
      int flowControls = context.getBeansOfType(PipelineFlowControl.class).size();
      // Ignore this, but if there are others, register for management
      if (hasInboundChannels || flowControls > 1) {
        Management.removeBeanOrFolder(getBeanName(), this);
        Management.addBean(getBeanName(), this);
      }
    }
  }

  @Override
  @ManagedOperation
  public void pause() {
    super.pause();
  }

  @Override
  @ManagedOperation
  public void resume() {
    super.resume();
  }

}
