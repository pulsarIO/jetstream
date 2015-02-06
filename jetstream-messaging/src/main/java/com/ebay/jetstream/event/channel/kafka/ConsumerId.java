/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.event.channel.kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * @author weifang
 * 
 */
public final class ConsumerId {
	private String m_consumerGroupId;

	private String m_hostName;

	private int m_uniqueId;

	public ConsumerId(String consumerGroupId) {
		this(consumerGroupId, null, null);
	}

	public ConsumerId(String consumerGroupId, String uniqueId) {
		this(consumerGroupId, null, uniqueId);
	}

	public ConsumerId(String consumerGroupId, String hostName, String uniqueId) {
		if (consumerGroupId == null) {
			throw new NullPointerException("consumerGroupId can't be null");
		}
		this.m_consumerGroupId = consumerGroupId;

		if (hostName == null) {
			try {
				InetAddress s = InetAddress.getLocalHost();
				this.m_hostName = s.getCanonicalHostName();
			} catch (UnknownHostException e) {
				throw new RuntimeException("Can resolve local host name", e);
			}
		}

		if (uniqueId == null) {
			this.m_uniqueId = Math.abs(UUID.randomUUID().hashCode());
		}
	}

	public String getConsumerGroupId() {
		return m_consumerGroupId;
	}

	public void setConsumerGroupId(String consumerGroupId) {
		this.m_consumerGroupId = consumerGroupId;
	}

	public String getHostName() {
		return m_hostName;
	}

	public void setHostName(String hostName) {
		this.m_hostName = hostName;
	}

	public int getUniqueId() {
		return m_uniqueId;
	}

	public void setUniqueId(int uniqueId) {
		this.m_uniqueId = uniqueId;
	}

	@Override
	public String toString() {
		return m_consumerGroupId + "_" + m_hostName + "-" + m_uniqueId;
	}

	@Override
	public int hashCode() {
		int ret = 31;
		ret += m_consumerGroupId.hashCode();
		ret += m_hostName.hashCode();
		ret += m_uniqueId;
		return ret;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ConsumerId)) {
			return false;
		}

		ConsumerId o = (ConsumerId) obj;
		if (!m_consumerGroupId.equals(o.m_consumerGroupId)) {
			return false;
		}

		if (!m_hostName.equals(o.m_hostName)) {
			return false;
		}

		if (m_uniqueId != o.m_uniqueId) {
			return false;
		}

		return true;
	}

}
