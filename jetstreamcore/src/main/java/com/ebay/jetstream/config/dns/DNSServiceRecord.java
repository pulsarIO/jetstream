/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.config.dns;

import java.util.StringTokenizer;

import com.ebay.jetstream.config.ConfigException;


public class DNSServiceRecord {
	private String m_rec;
	private int m_port;
	private int m_priority;
	private String m_target;
	private int m_weight;

	public DNSServiceRecord(String rec) throws ConfigException {
		StringTokenizer toks = new StringTokenizer(rec);
		if (toks.countTokens() < 4) {
			throw new ConfigException("Invalid service record encountered: " + rec);
		}

		m_rec = rec;
		m_priority = Integer.parseInt(toks.nextToken());
		m_weight = Integer.parseInt(toks.nextToken());
		m_port = Integer.parseInt(toks.nextToken());
		m_target = toks.nextToken();
	}

	public DNSServiceRecord() {}

	/**
	 * @return the original SRV string
	 */
	public String getSRVString() {
		return m_rec;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return m_port;
	}

	/**
	 * @param port the port to set
	 */
	void setPort(int port) {
		m_port = port;
	}

	/**
	 * @return the priority
	 */
	public int getPriority() {
		return m_priority;
	}

	/**
	 * @param priority the priority to set
	 */
	void setPriority(int priority) {
		m_priority = priority;
	}

	/**
	 * @return the target
	 */
	public String getTarget() {
		return m_target;
	}

	/**
	 * @param target the target to set
	 */
	void setTarget(String target) {
		m_target = target;
	}

	/**
	 * @return the weight
	 */
	public int getWeight() {
		return m_weight;
	}

	/**
	 * @param weight the weight to set
	 */
	void setWeight(int weight) {
		m_weight = weight;
	}
}