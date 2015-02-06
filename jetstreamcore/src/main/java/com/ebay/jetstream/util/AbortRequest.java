/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

/**
* An implementation of a request thread pattern 
*
* *
* @author shmurthy@ebay.com
* @version 1.0
*/ 




public class AbortRequest extends Request {
 
	public AbortRequest() {}

	public boolean execute() { return false; }
	
}
