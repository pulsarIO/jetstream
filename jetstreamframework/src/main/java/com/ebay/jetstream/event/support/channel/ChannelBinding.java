/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.support.channel;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

import com.ebay.jetstream.event.channel.ChannelAlarm;
import com.ebay.jetstream.event.channel.ChannelAlarmListener;
import com.ebay.jetstream.event.channel.ChannelOperations;
import com.ebay.jetstream.event.channel.InboundChannel;
import com.ebay.jetstream.event.channel.OutboundChannel;

public class ChannelBinding extends PipelineFlowControl implements ApplicationListener, InitializingBean {
  private class ChannelHandler implements ChannelAlarmListener {

    public void alarm(ChannelAlarm alarm) {
      if (alarm == ChannelAlarm.OVERRUN) {
        pause();
      }
      else if (alarm == ChannelAlarm.CLEAR) {
        resume();
      }
    }
  }

  private ChannelOperations m_channel;
  private RemoteController m_remoteController;

  public void setRemoteController(RemoteController remoteController) {
       this.m_remoteController = remoteController;
  }

  public ChannelBinding() {

  }

  public void afterPropertiesSet() throws Exception {
    m_channel.open();
    if (m_remoteController != null && m_channel instanceof InboundChannel) {
        m_remoteController.setInboundChannel((InboundChannel) m_channel);
        m_remoteController.subscribe();
    }
  }

  public ChannelOperations getChannel() {
    return m_channel;
  }

  public void onApplicationEvent(ApplicationEvent event) {
    if (event instanceof ContextClosedEvent) {
      m_channel.close();
    }
    /*
     * else if (event instanceof PauseEvent || event instanceof ResumeEvent) { if (m_channel instanceof InboundChannel)
     * { InboundChannel in = (InboundChannel) m_channel; if (event instanceof PauseEvent) { in.pause(); } else if (event
     * instanceof ResumeEvent) { in.resume(); } } }
     */
  }

  public void setChannel(ChannelOperations channel) {
    m_channel = channel;
    if (m_channel instanceof OutboundChannel) {
      ((OutboundChannel) m_channel).setAlarmListener(new ChannelHandler());
    }
  }

}
