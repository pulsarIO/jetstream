/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs.stats;

import java.util.List;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author weifang
 * 
 */
public class EventTsBasedSuccessCheckerConfig extends AbstractNamedBean
		implements XSerializable {
	private String outputFolder;
	private String identifier;
	private String dataCenter;
	private List<String> totalTopics;
	private List<String> totalDataCenters;
	private int successCheckCount = 6;
	private long successCheckInterval = 300000;
	private String successFileName = "_SUCCESS";
	private String zkHosts;
	private int zkConnectionTimeoutMs = 5000;
	private int zkSessionTimeoutMs = 10000;
	private int zkRetryTimes = 3;
	private int zkSleepMsBetweenRetries = 5000;

	public String getOutputFolder() {
		return outputFolder;
	}

	public void setOutputFolder(String outputFolder) {
		this.outputFolder = outputFolder;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public String getDataCenter() {
		return dataCenter;
	}

	public void setDataCenter(String dataCenter) {
		this.dataCenter = dataCenter;
	}

	public List<String> getTotalTopics() {
		return totalTopics;
	}

	public void setTotalTopics(List<String> totalTopics) {
		this.totalTopics = totalTopics;
	}

	public List<String> getTotalDataCenters() {
		return totalDataCenters;
	}

	public void setTotalDataCenters(List<String> totalDataCenters) {
		this.totalDataCenters = totalDataCenters;
	}

	public int getSuccessCheckCount() {
		return successCheckCount;
	}

	public void setSuccessCheckCount(int successCheckCount) {
		this.successCheckCount = successCheckCount;
	}

	public String getSuccessFileName() {
		return successFileName;
	}

	public void setSuccessFileName(String successFileName) {
		this.successFileName = successFileName;
	}

	public long getSuccessCheckInterval() {
		return successCheckInterval;
	}

	public void setSuccessCheckInterval(long successCheckInterval) {
		this.successCheckInterval = successCheckInterval;
	}

	public String getZkHosts() {
		return zkHosts;
	}

	public void setZkHosts(String zkHosts) {
		this.zkHosts = zkHosts;
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

	public int getZkRetryTimes() {
		return zkRetryTimes;
	}

	public void setZkRetryTimes(int zkRetryTimes) {
		this.zkRetryTimes = zkRetryTimes;
	}

	public int getZkSleepMsBetweenRetries() {
		return zkSleepMsBetweenRetries;
	}

	public void setZkSleepMsBetweenRetries(int zkSleepMsBetweenRetries) {
		this.zkSleepMsBetweenRetries = zkSleepMsBetweenRetries;
	}

}
