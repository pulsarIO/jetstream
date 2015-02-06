/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import static com.ebay.jetstream.event.BatchSinkRequest.AdvanceAndGetNextBatch;
import static com.ebay.jetstream.event.BatchSinkRequest.GetNextBatch;
import static com.ebay.jetstream.event.BatchSinkRequest.RevertAndGetNextBatch;

/**
 * @author xiaojuwu1
 */
public class BatchResponse {

	private BatchSinkRequest request;
	private long offset = -1;
	private long waitTimeInMs = 0;
	private int batchSizeBytes = -1;

	public static BatchResponse getNextBatch() {
		return new BatchResponse(GetNextBatch);
	}
	
	public static BatchResponse advanceAndGetNextBatch() {
		return new BatchResponse(AdvanceAndGetNextBatch);
	}
	
	public static BatchResponse revertAndGetNextBatch() {
		return new BatchResponse(RevertAndGetNextBatch);
	}

	private BatchResponse() {
	}

	private BatchResponse(BatchSinkRequest request) {
		this.request = request;
	}

	private BatchResponse(BatchSinkRequest request, long offset, long waitTimeInMs,
			int batchSizeBytes) {
		this.request = request;
		this.offset = offset;
		this.waitTimeInMs = waitTimeInMs;
		this.batchSizeBytes = batchSizeBytes;
	}

	public BatchSinkRequest getRequest() {
		return request;
	}

	public BatchResponse setRequest(BatchSinkRequest request) {
		this.request = request;
		return this;
	}

	public long getOffset() {
		return offset;
	}

	public BatchResponse setOffset(long offset) {
		this.offset = offset;
		return this;
	}

	public long getWaitTimeInMs() {
		return waitTimeInMs;
	}

	public BatchResponse setWaitTimeInMs(long waitTimeInMs) {
		this.waitTimeInMs = waitTimeInMs;
		return this;
	}

	public int getBatchSizeBytes() {
		return batchSizeBytes;
	}

	public BatchResponse setBatchSizeBytes(int batchSizeBytes) {
		this.batchSizeBytes = batchSizeBytes;
		return this;
	}

}
