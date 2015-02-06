/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

import com.ebay.jetstream.util.TimeSlotHashMap;
import com.ebay.jetstream.util.TimeSlotHashMap.Key;

public class RequestId {

  public static RequestId newRequestId() {
    RequestId reqid = new RequestId();
    reqid.setId(TimeSlotHashMap.genKey());
    return reqid;
  }

  private Key m_id;

  /**
   * @return the id
   */
  public long getId() {
    return m_id.getGuid();
  }

  Key getKey() {
    return m_id;
  }

  /**
   * @param id
   *          the id to set
   */
  private void setId(Key id) {
    m_id = id;
  }

  @Override
  public String toString() {
    return m_id.toString();
  }

}
