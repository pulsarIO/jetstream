/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.ebay.jetstream.event.channel.ChannelAddress;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;


/**
 * @author shmurthy
 *
 *  MessageChannelAddress -  a Class that represents the address of a MessageChannel
 *  A message channel address consists of a list of JetstreamTopics                        
 */

public class MessagingChannelAddress extends ChannelAddress {
 
  private CopyOnWriteArrayList<String> m_channelTopics = new CopyOnWriteArrayList<String>();
  private CopyOnWriteArrayList<JetstreamTopic> m_channelJetstreamTopics = new CopyOnWriteArrayList<JetstreamTopic>();

  /**
   * 
   */
  public MessagingChannelAddress() {

  }



  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    StringBuffer addressStr = new StringBuffer();
    addressStr.append("Topic List - \n");

    for (String topic : m_channelTopics) 
    {
      addressStr.append(topic);
      addressStr.append(" ; ");
    }

    return addressStr.toString();
  }


  /**
   * 
   * @param String
   * @return boolean
   */
  public boolean contains(String str) {
    return (m_channelTopics.contains(str));
  }
  
  /**
   * @return the channelTopics
   */
  public List<String> getChannelTopics() {
    return m_channelTopics;
  }

  /**
   * @return the channelJetstreamTopics
   */
  public List<JetstreamTopic> getChannelJetstreamTopics() {
    return m_channelJetstreamTopics;
  }

  /**
   * @param channelTopics the channelTopics to set
   */
  public void setChannelTopics(List<String> channelTopics) {

    m_channelTopics.clear();

    m_channelJetstreamTopics.clear();

    for (String topic : channelTopics) {
      m_channelTopics.add(topic);
      m_channelJetstreamTopics.add(new JetstreamTopic(topic));
    }
  }

}
