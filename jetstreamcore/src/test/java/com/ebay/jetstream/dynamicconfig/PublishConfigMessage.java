/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.dynamicconfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.ConfigChangeMessage;
import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;

public class PublishConfigMessage {
	private static final String s_topic = "Rtbdpod.local/notification";
	private static final Logger LOGGER = LoggerFactory.getLogger(PublishConfigMessage.class.getPackage().getName());

	private MessageService m_messageService;

	public PublishConfigMessage() {
		 m_messageService = MessageService.getInstance();
	}


	public void publish(String application, String scope, String version,
			String beanId, String beanVersion) {
		if (m_messageService.isInitialized()) {
			try {
				String configChangeEventTopic = ConfigUtils
						.getPropOrEnv("CONFIGNOTIFICATIONTOPIC");
				if (configChangeEventTopic == null
						|| configChangeEventTopic.equals(""))
					configChangeEventTopic = s_topic;

				m_messageService.publish(new JetstreamTopic(
						configChangeEventTopic), new ConfigChangeMessage(
						application, scope, version, beanId, beanVersion));

				LOGGER.info( "Publish SUCCESSFUL for bean : " + beanId);

				// Sleep for 3 second to give Application to update itself.
				Thread.sleep(3000);

			} catch (Exception e) {
				LOGGER.info( "Fatal Error : Publish method failed to broadcast the message" +e.getMessage(), e);
			}
		} else {
			LOGGER.info( "Fatal Error : Message Service Not initialized");
		}
	}

}

