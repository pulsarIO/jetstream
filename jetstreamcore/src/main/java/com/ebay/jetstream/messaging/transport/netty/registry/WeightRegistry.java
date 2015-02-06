/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.registry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;

import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 *          This maintains a list of all discovered weight values.
 * 
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="SBSC_USE_STRINGBUFFER_CONCATENATION")	
public class WeightRegistry implements XSerializable, Externalizable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private LinkedList<Long> m_weightList = new LinkedList<Long>();
	private long m_maxWeight = -1;
	private long m_weightGcd = -1;

	/**
	 * @return the maxWeight
	 */
	public long getMaxWeight() {
		return m_maxWeight;
	}

	/**
	 * @param maxWeight
	 *            the maxWeight to set
	 */
	public void setMaxWeight(long maxWeight) {
		m_maxWeight = maxWeight;
	}

	/**
	 * @return the weightDb
	 */
	public LinkedList<Long> getWeights() {
		return m_weightList;
	}

	/**
	 * @return the weightGcd
	 */
	public long getWeightGcd() {
		return m_weightGcd;
	}

	/**
	 * @param weightGcd
	 *            the weightGcd to set
	 */
	public void setWeightGcd(long weightGcd) {
		m_weightGcd = weightGcd;
	}

	/**
	 * @param weight
	 */
	public void add(long weight) {
		Long newWeight = Long.valueOf(weight);

		if (!m_weightList.contains(newWeight)) {
			m_weightList.add(newWeight);

		}

		computeWeightGcd();
		computeMaxWeight();
	}

	/**
	 * @param weight
	 */
	public void remove(long weight) {
		Long newWeight = Long.valueOf(weight);

		if (m_weightList.contains(newWeight)) {
			m_weightList.remove(newWeight);

		}

		computeWeightGcd();
		computeMaxWeight();
	}

	/**
	 * @param x
	 * @param y
	 * @return
	 */
	private long calcGcd(long x, long y) {

		while (y != 0) {
			long z = y;
			y = x % y;
			x = z;
		}
		return x;

	}

	/**
	 * 
	 */
	private void computeWeightGcd() {
		int i;

		if (m_weightList.size() == 0) {
			m_weightGcd = -1;
			return;
		}

		m_weightGcd = m_weightList.get(0).longValue();

		for (i = 1; i < m_weightList.size(); i++)
			m_weightGcd = calcGcd(m_weightGcd, m_weightList.get(i).longValue());

	}

	/**
	 * 
	 */
	private void computeMaxWeight() {
		for (int j = 0; j < m_weightList.size(); j++)
			m_maxWeight = Math
					.max(m_weightList.get(j).longValue(), m_maxWeight);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		String weightListStr = "Available Weights = ";

		if (m_weightList.size() == 0) {
			weightListStr += "None";
			return weightListStr;

		}

		weightListStr += Long.toString(m_weightList.get(0).longValue());

		for (int i = 1; i < m_weightList.size(); i++) {
			weightListStr += ",";
			weightListStr += Long.toString(m_weightList.get(i).longValue());
		}

		weightListStr += "\nGCD = ";
		weightListStr += m_weightGcd;
		weightListStr += "\nmaxWeight = ";
		weightListStr += m_maxWeight;

		return weightListStr;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		m_maxWeight = in.readLong();
		m_weightGcd = in.readLong();
		m_weightList = (LinkedList<Long>) in.readObject();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(m_maxWeight);
		out.writeLong(m_weightGcd);
		out.writeObject(m_weightList);
	}
}
