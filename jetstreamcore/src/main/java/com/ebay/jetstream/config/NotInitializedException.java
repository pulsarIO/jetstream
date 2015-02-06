/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

public class NotInitializedException extends ConfigException {
	private static final long serialVersionUID = 1L;

	public NotInitializedException() {
		super();
	}

	public NotInitializedException(String message, Throwable cause) {
		super(message, cause);
	}

	public NotInitializedException(String message) {
		super(message);
	}

	public NotInitializedException(Throwable cause) {
		super(cause);
	}
}
