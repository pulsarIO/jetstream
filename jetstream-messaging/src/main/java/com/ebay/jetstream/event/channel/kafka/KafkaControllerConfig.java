/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

public class KafkaControllerConfig extends AbstractNamedBean implements
		XSerializable {

	private int rebalanceInterval = 60000;
	private String zkConnect;
	private int zkConnectionTimeoutMs = 20000;
	private int zkSessionTimeoutMs = 30000;
	private int retryTimes = 2;
	private int sleepMsBetweenRetries = 500;

	private int rebalanceableWaitInMs = 60000;

	public int getRebalanceInterval() {
		return rebalanceInterval;
	}

	public void setRebalanceInterval(int rebalanceInterval) {
		this.rebalanceInterval = rebalanceInterval;
	}

	public String getZkConnect() {
		return zkConnect;
	}

	public void setZkConnect(String zkConnect) {
		this.zkConnect = zkConnect;
	}

	public int getZkConnectionTimeoutMs() {
		return zkConnectionTimeoutMs;
	}

	public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
		this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
	}

	public int getZkSessionTimeoutMs() {
		return zkSessionTimeoutMs;
	}

	public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
		this.zkSessionTimeoutMs = zkSessionTimeoutMs;
	}

	public int getRetryTimes() {
		return retryTimes;
	}

	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}

	public int getSleepMsBetweenRetries() {
		return sleepMsBetweenRetries;
	}

	public void setSleepMsBetweenRetries(int sleepMsBetweenRetries) {
		this.sleepMsBetweenRetries = sleepMsBetweenRetries;
	}

	public int getRebalanceableWaitInMs() {
		return rebalanceableWaitInMs;
	}

	public void setRebalanceableWaitInMs(int rebalanceableWaitInMs) {
		this.rebalanceableWaitInMs = rebalanceableWaitInMs;
	}

}
