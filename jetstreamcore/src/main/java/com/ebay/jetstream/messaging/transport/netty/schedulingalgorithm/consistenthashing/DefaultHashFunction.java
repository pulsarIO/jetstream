/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing;


import java.security.NoSuchAlgorithmException;


/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          Deafult hashing algorithm
 * 
 */
public class DefaultHashFunction implements HashFunction {

	MurmurHashFunction m_mhf = new MurmurHashFunction();
	
	/**
	 * @throws NoSuchAlgorithmException
	 */
	public DefaultHashFunction() throws NoSuchAlgorithmException {
	
	}
	
	/* (non-Javadoc)
	 * @see com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing.HashFunction#hash(java.lang.Object)
	 */
	@Override
	
	public Long hash(Object val) {
		
		return m_mhf.hash(val);
	}

}
