/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

/**
 * @author xiaojuwu1
 */
public class EventMetaInfo {

	private BatchSourceCommand action;
	private BatchSource batchSource;
	private Exception exception;
	private BatchResponse batchResponse;

	public EventMetaInfo() {
	}
	
	public EventMetaInfo(BatchSourceCommand action, BatchSource batchSource) {
		this.action = action;
		this.batchSource = batchSource;
	}

	public EventMetaInfo(BatchSourceCommand action, BatchSource batchSource, Exception ex) {
		this.action = action;
		this.batchSource = batchSource;
		this.exception = ex;
	}

	public BatchSourceCommand getAction() {
		return action;
	}

	public void setAction(BatchSourceCommand action) {
		this.action = action;
	}

	public BatchSource getBatchSource() {
		return batchSource;
	}

	public void setBatchSource(BatchSource batchSource) {
		this.batchSource = batchSource;
	}

	public Exception getException() {
		return exception;
	}

	public void setException(Exception exception) {
		this.exception = exception;
	}

	public BatchResponse getBatchResponse() {
		return batchResponse;
	}

	public void setBatchResponse(BatchResponse batchResponse) {
		this.batchResponse = batchResponse;
	}

}
