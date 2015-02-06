/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class MurmurHashFunction implements com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing.HashFunction  {

	HashFunction m_hf;
	int m_seed = 2147368987;
	
	public int getSeed() {
		return m_seed;
	}

	public void setSeed(int seed) {
		this.m_seed = seed;
		m_hf = Hashing.murmur3_128(m_seed);
	}

	public MurmurHashFunction() {
		m_hf = Hashing.murmur3_128(m_seed);
	}
	
	public Long hash(Object val) {
		
	    long hash;
	    HashCode hc;
	    
		if (val instanceof String)
        	hc = m_hf.newHasher().putString((String) val).hash();
        else if (val instanceof Long) 
        	hc = m_hf.newHasher().putLong((Long) val).hash();
        else if (val instanceof Integer)
        	hc = m_hf.newHasher().putInt((Integer) val).hash();
        else if (val instanceof Boolean)
        	hc = m_hf.newHasher().putBoolean((Boolean) val).hash();
        else if (val instanceof byte[])
        	hc = m_hf.newHasher().putBytes((byte[]) val).hash();
        else 
        	hc = m_hf.newHasher().putInt(val.hashCode()).hash();
		return hc.asLong();
		
		
	}
	
}
