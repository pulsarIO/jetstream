/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.management;

import java.util.Map;

public interface ControlBean {
  Object process(Map<String, String[]> parameters) throws Exception;
}
