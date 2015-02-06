/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.consistenthashing;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;


/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          Implements ConsistentHashing Algorithm - this is implemented as suggested in the original MIT paper on consistent hashing
 *          with a slight modifucation. For each point to be added to the continum we compute a provisioned number of hashes and add the 
 *          hashes to the continum. We take the String value of the passed point and add a delta to it. The delta is determined by the
 *          spread factor which is provisioned. The hash value of the original + delta gives us a new point to be inserted to the continum.
 *          This way the hashes are spread all over the continum. This provides us a way to spread the traffic across the cluster when we
 *          have to rebalance the traffic from a failed node across nodes in the cluster. 
 * 
 */
public class ConsistentHashing<T> implements XSerializable {

	private HashFunction m_hashFunction;
	private int m_numHashesPerEntry = 1000;
	private final SortedMap<Long, T> m_continum = new TreeMap<Long, T>();
	private T[] m_consumerArray = (T[]) new Object[0];
	private long[] m_pointArray = new long[0];
	private long m_spreadFactor = 100000;
	
	private final static int INDEX_LENGTH = 128; // Create an index to find the
												 // range fast.
	private long[] m_indexArray;
	private int m_segmentSize;

	@SuppressWarnings("unchecked")
	public void convert() {
		int size = m_continum.size();
		m_pointArray = new long[size];
		m_consumerArray = (T[]) new Object[size];
		int c = 0;
		for (Iterator<Map.Entry<Long, T>> iter = m_continum.entrySet().iterator(); iter.hasNext();) {
			Entry<Long, T> e = iter.next();
			Long key = e.getKey();
			T t = e.getValue();
			m_pointArray[c] = key;
			m_consumerArray[c] = t;
			c++;
		}
		if (c > INDEX_LENGTH * 4) {
			m_indexArray = new long[INDEX_LENGTH];
			m_segmentSize = c / INDEX_LENGTH;
			for (int i = 0, t = INDEX_LENGTH; i < t; i++) {
				m_indexArray[i] = m_pointArray[i * m_segmentSize];
			}
		} else {
			m_indexArray = null;
		}
	}
	/**
	 * @return
	 */
	public long getSpreadFactor() {
		return m_spreadFactor;
	}

	/**
	 * @param m_spreadFactor
	 */
	public void setSpreadFactor(long m_spreadFactor) {
		this.m_spreadFactor = m_spreadFactor;
	}

	/**
	 * 
	 */
	public ConsistentHashing() {
	}

	/**
	 * @return
	 */
	public HashFunction getHashFunction() {
		return m_hashFunction;
	}

	/**
	 * @param m_hashFunction
	 */
	public void setHashFunction(HashFunction m_hashFunction) {
		this.m_hashFunction = m_hashFunction;
	}

	/**
	 * @return
	 */
	public int getNumHashesPerEntry() {
		return m_numHashesPerEntry;
	}

	/**
	 * @param m_numHashesPerEntry
	 */
	public void setNumHashesPerEntry(int m_numHashesPerEntry) {
		this.m_numHashesPerEntry = m_numHashesPerEntry;
	}

	/**
	 * @return
	 */
	@Hidden
	// public Map<Integer, T> getContinum() {
	public Map<Long, T> getContinum() {
		return (Map<Long, T>) Collections.unmodifiableMap(m_continum);
	}

	/**
	 * @param hashFunction
	 * @param numHashesPerEntry
	 * @param spreadFactor
	 */
	public ConsistentHashing(HashFunction hashFunction, int numHashesPerEntry,
			long spreadFactor) {
		m_hashFunction = hashFunction;
		m_numHashesPerEntry = numHashesPerEntry;
		m_spreadFactor = spreadFactor;

	}

	/**
	 * @param point
	 */
	public void add(T point) {
		
		for (int i = 0; i < m_numHashesPerEntry; i++) {
			m_continum.put(m_hashFunction.hash(point.toString()
					+ m_spreadFactor * i), point);
		}
		convert();
	}

	/**
	 * @param point
	 */
	public void remove(T point) {
		for (int i = 0; i < m_numHashesPerEntry; i++) {
			m_continum.remove(m_hashFunction.hash(point.toString()
					+ (m_spreadFactor * i)));
		}
		convert();
	}

	private int lookUp(long p) {
		int start = 0;
		int end = m_pointArray.length;
		
		if (m_indexArray != null) {
			int mid;
			end = INDEX_LENGTH;
			while (start + 1 < end) {
				mid = (start + end) >> 1;
				long v = m_pointArray[mid * m_segmentSize];
				if (p == v) {
					return mid * m_segmentSize;
				} else if (p > v) {
					start = mid;
				} else {
					end = mid;
				}
			}

			start = start * m_segmentSize;
			if (end == INDEX_LENGTH) {
				end = m_pointArray.length;
			} else {
				end = end * m_segmentSize;
			}
		}
		
		int mid;
		while (start + 1 < end) {
			mid = (start + end) >> 1;
			long v = m_pointArray[mid];
			if (p == v) {
				return mid;
			} else if (p > v) {
				start = mid;
			} else {
				end = mid;
			}
		}
		return end;
	}
	
	/**
	 * @param key
	 * @return
	 */
	public T get(Object key) {
		if (m_pointArray.length == 0) {
			return null;
		}
		
		long hash = m_hashFunction.hash(Long.toString(key.hashCode()));
		if (hash <= m_pointArray[0]) {
			return m_consumerArray[0];
		} else if (hash >= m_pointArray[m_pointArray.length -1]) {
			return m_consumerArray[0];
		}
		int pos = lookUp(hash);
		return m_consumerArray[pos];
	}

	/**
	 * 
	 */
	public void clear() {
		m_continum.clear();
		m_consumerArray = (T[]) new Object[0];
		m_pointArray = new long[0];
		m_indexArray = null;
		m_segmentSize = 0;
	}
	
	/**
	 * @return
	 */
	public boolean isEmpty() {
		return m_continum.isEmpty();
	}

}