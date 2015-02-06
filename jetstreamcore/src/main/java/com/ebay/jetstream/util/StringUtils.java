/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.util.ArrayList;
import java.util.List;

public class StringUtils {

  private static final String STRING_LIST_SEPARATOR = ",";

  /**
   * Converts comma-separated string into a list. Performs trimming, ignores empty strings. For EPL like<br>
   * <code> 
   * SELECT com.ebay.jetstream.util.StringUtils.convertToList('One, Two, Three, ') AS TopicList FROM ESPTestEvent1
   * </code><br>
   * it returns a list of three strings: "One", "Two", "Three"
   * 
   * @param vars
   * @return
   */
  public static List<String> convertToList(String vars) {
    List<String> resultList = new ArrayList<String>();
    String[] resultArray = vars.split(STRING_LIST_SEPARATOR);
    for (String var : resultArray) {
      var = var.trim();
      if (var.length() > 0) {
        resultList.add(var);
      }
    }
    return resultList;
  }

}
