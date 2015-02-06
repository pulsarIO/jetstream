/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;

/**
 * @author shmurthy
 *
 *
 */

public class ListenerInfo {

		public IMessageListener m_listener;
		public JetstreamTopic m_topic;
		
		/**
		 * 
		 */
		public ListenerInfo() {}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		public boolean equals(Object obj)
		{
			if (this == obj) return true;
			
			if (obj == null) return false;
			
			if (!(obj instanceof ListenerInfo)) return false;
			
			ListenerInfo li = (ListenerInfo) obj;
			
			
			if (!m_topic.getTopicName().equals(li.m_topic.getTopicName()))
				return false;
			if (li.m_listener != m_listener)
				return false;
			
			return true;
		}
		
		public int hashCode() {
			return m_topic.hashCode();
		}
}
