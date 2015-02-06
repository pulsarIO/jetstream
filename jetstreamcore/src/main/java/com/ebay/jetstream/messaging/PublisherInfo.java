/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author shmurthy
 *
 *
 */

public class PublisherInfo {

	private AtomicLong m_prevSeqId;
    private AtomicLong m_lastUpdateTime = new AtomicLong(0);


	/**
	 * 
	 */
	public PublisherInfo() {}

	/**
	 * @param seqId
	 */
	public PublisherInfo(long seqId)
	{
		m_prevSeqId = new AtomicLong(seqId);
        m_lastUpdateTime.set(System.currentTimeMillis());

	}

	/**
	 * @param seqId
	 */
	public void setSeqId(long seqId)
	{
		m_prevSeqId = new AtomicLong(seqId);
        m_lastUpdateTime.set(System.currentTimeMillis());
	}

	/**
	 * @param seqId
	 * @return
	 */
	public boolean isSeqIdMismatch(long seqId)
	{
		if (seqId == 0) 
          {
            m_prevSeqId.set(0);
		    return false;
          }

		
		if (m_prevSeqId.compareAndSet(seqId - 1, seqId))
		{
				return false;

		}
		
		m_prevSeqId.set(seqId);
        m_lastUpdateTime.set(System.currentTimeMillis());
		return true;

	}

	/**
	 * @return
	 */
	public long getSeqId()
	{
		return m_prevSeqId.get();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {

		if (this == obj) return true;

		if (obj == null) return false;

		if (!(obj instanceof PublisherInfo))
			return false;

		PublisherInfo key = (PublisherInfo) obj;

		if (m_prevSeqId != key.m_prevSeqId)
			return false;

		return true;
	}
	
	public int hashCode() {
		return m_prevSeqId.hashCode() + m_lastUpdateTime.hashCode();
	}

  /**
   * @return the lastUpdateTime
   */
  public long getLastUpdateTime() {
    return m_lastUpdateTime.get();
  }

  /**
   * @param lastUpdateTime the lastUpdateTime to set
   */
  public void setLastUpdateTime(long lastUpdateTime) {
    m_lastUpdateTime.set(lastUpdateTime);
  }

}
