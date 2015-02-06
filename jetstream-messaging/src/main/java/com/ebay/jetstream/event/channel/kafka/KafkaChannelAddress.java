/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.ebay.jetstream.event.channel.ChannelAddress;

/**
 * Channel address used by kafka channel.
 *  
 * @author xingwang
 * 
 */

public class KafkaChannelAddress extends ChannelAddress {
	private CopyOnWriteArrayList<String> m_channelTopics = new CopyOnWriteArrayList<String>();

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
	 * @param channelTopics
	 *            the channelTopics to set
	 */
	public void setChannelTopics(List<String> channelTopics) {

		m_channelTopics.clear();

		for (String topic : channelTopics) {
			m_channelTopics.add(topic);
		}
	}

	@Override
	public String toString() {
		StringBuffer addressStr = new StringBuffer();
		addressStr.append("Topic List - \n");

		for (String topic : m_channelTopics) {
			addressStr.append(topic);
			addressStr.append(" ; ");
		}

		return addressStr.toString();
	}

}
