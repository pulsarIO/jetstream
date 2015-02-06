/*******************************************************************************
 * Copyright 2012-2015 eBay Software Foundation
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ebay.jetstream.mongo.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.ConfigChangeMessage;
import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;

public class PublishConfigMessage {

	private static final String s_topic = "Rtbdpod.local/notification";
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PublishConfigMessage.class.getName());

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

				LOGGER.info("Publish SUCCESSFUL for bean : " + beanId);

				// Sleep for 3 second to give Application to update itself.
				Thread.sleep(3000);

			} catch (Exception e) {
				LOGGER.info("Fatal Error : Publish method failed to broadcast the message" +e.getMessage(), e);
			}
		} else {
			LOGGER.info("Fatal Error : Message Service Not initialized");
		}
	}

}

