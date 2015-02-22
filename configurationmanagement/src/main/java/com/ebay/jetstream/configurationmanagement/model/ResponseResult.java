/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.model;


public class ResponseResult {
	private boolean isSuccess;
	private String message;
	private Object payload;

	public ResponseResult(boolean isSucc) {
		this(isSucc, null, null);
	}

	public ResponseResult(boolean isSucc, String msg) {
		this(isSucc, msg, null);
	}

	public ResponseResult(boolean isSucc, String msg, Object payload) {
		this.isSuccess = isSucc;
		this.message = msg;
		this.payload = payload;
	}

	public boolean isSuccess() {
		return isSuccess;
	}
	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}

	public Object getPayload() {
		return payload;
	}

	public void setPayload(Object payload) {
		this.payload = payload;
	}
	
	
}
