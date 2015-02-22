/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.model;

import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;

public class JetStreamBeanConfigurationLogDo extends
		JetStreamBeanConfigurationDo {
	private int status;
	private String operatedBy;
	private long operatedDate;

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getOperatedBy() {
		return operatedBy;
	}

	public void setOperatedBy(String operatedBy) {
		this.operatedBy = operatedBy;
	}

	public long getOperatedDate() {
		return operatedDate;
	}

	public void setOperatedDate(long operatedDate) {
		this.operatedDate = operatedDate;
	}

}
