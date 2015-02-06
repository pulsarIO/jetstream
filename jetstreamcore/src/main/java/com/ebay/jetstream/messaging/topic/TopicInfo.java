/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.topic;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.util.GuidGenerator;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DMI_RANDOM_USED_ONLY_ONCE")
public class TopicInfo {

  public long m_id;
  public AtomicLong m_seqId;
  private Random m_random = new SecureRandom();

  /**
		 * 
		 */
  public TopicInfo() {
    m_id = GuidGenerator.gen();
    m_seqId = new AtomicLong(m_random.nextLong());
  }

  /**
   * @return
   */
  public long incSeqId() {
    return m_seqId.incrementAndGet();
  }

}
