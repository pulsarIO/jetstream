/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import java.util.List;

import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.interfaces.ITransportListener;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.eventconsumer.EventConsumer;
import com.ebay.jetstream.messaging.transport.netty.eventproducer.EventProducer;

/**
 * @author shmurthy
 *
 *  Proxy for Transports to subscribe to messages.
 */

public final class MessageServiceProxy implements ITransportListener {

  private final MessageService m_messageService;

  public MessageServiceProxy(MessageService service) {
    m_messageService = service;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ebay.jetstream.messaging.TransportListener#postAdvise(com.ebay.jetstream.messaging.JetstreamMessage)
   */
  public void postAdvise(JetstreamMessage tm) {
    if (m_messageService != null) {
      m_messageService.postAdvise(tm);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ebay.jetstream.messaging.TransportListener#receive(com.ebay.jetstream.messaging.JetstreamMessage,
   * com.ebay.jetstream.messaging.DispatchQueueStats)
   */
  public void postMessage(JetstreamMessage tm, DispatchQueueStats stats) throws MessageServiceException {
    if (m_messageService != null) {
      m_messageService.postMessage(tm, stats);
    }

  }

  /**
   * @param topic
   * @param tm
   * @param who
   * @throws java.lang.Exception
   */
  public void publish(JetstreamTopic topic, JetstreamMessage tm, Object who) throws java.lang.Exception {
    if (who.getClass() != EventProducer.class
        && who.getClass() != EventConsumer.class)
      throw new MessageServiceException(MessageServiceException.PERMISSION_DENIED, who.getClass()
          + " not allowed to execute this method");

    m_messageService.dispatchDownStream(topic, tm);
  }

  /**
   * @param topic
   * @param tml
   * @param who
   * @throws java.lang.Exception
   */
  public void subscribe(JetstreamTopic topic, IMessageListener tml, Object who) throws java.lang.Exception {
    if (who.getClass() != EventProducer.class
        && who.getClass() != EventConsumer.class)
      throw new MessageServiceException(MessageServiceException.PERMISSION_DENIED, who.getClass()
          + " not allowed to execute this method");

    m_messageService.createDispatcherRegisterWithTransport(topic);
    m_messageService.addSubscriber(topic, tml);
  }

    @Override
    public void postMessage(List<JetstreamMessage> msgs, DispatchQueueStats m_queueStats)
            throws MessageServiceException {
        if (m_messageService != null) {
            m_messageService.postMessage(msgs, m_queueStats);
        }

    }
}
