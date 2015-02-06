/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;

import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.util.Request;
import com.netflix.curator.framework.CuratorFramework;

public class WatchTopicRequest extends Request{
	
	ZooKeeperTransport m_transport;
	CuratorFramework m_handle;
	JetstreamTopic m_topic;
	
	WatchTopicRequest(ZooKeeperTransport transport, CuratorFramework handle,JetstreamTopic topic){
		m_transport = transport;
		m_handle = handle;
		m_topic = topic;
	}

	@Override
	public boolean execute() {
		m_transport.setWatchOnZkClient(m_handle, m_topic);
		return true;
	}

}
