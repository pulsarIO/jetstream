/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;


public class MongoConfigRuntimeException extends RuntimeException {

	public MongoConfigRuntimeException(String strMsg)
	{
		super(strMsg);
	}
	
	public MongoConfigRuntimeException(Throwable cause)
	{
		super(cause.getMessage(), cause);
	}
		
	public MongoConfigRuntimeException(String stringMessage, Throwable cause) {
		super(stringMessage, cause);	
	}
	
}