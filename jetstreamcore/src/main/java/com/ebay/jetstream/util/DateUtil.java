/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Miscellaneous date-time support routines.
 * 
 * 
 */
public class DateUtil {

  /**
   * Parses a specific ISO 8601 extended-format date-time string, format == "yyyy-MM-dd'T'HH:mm:ss" and returns the
   * resulting Date object.
   * 
   * FIX: In order to make EPL nested calls like:
   * com.ebay.jetstream.util.DateUtil.getMillisFromISO8601(com.ebay.jetstream.epl
   * .EPLUtilities.getAttributeValue(attributes.values, 'marketplace.transaction_date')) possible, we need this guy to
   * be able to accept Object which is a string actually
   * 
   * FIX for 5827 - we have support both 'T'-separated and ' '-separated formats
   * 
   * FIX for 11878 - now supported all 4 possible formats
   * 
   * @param iso8601
   *          an extended format ISO 8601 date-time string.
   * 
   * @return the Date object for the ISO 8601 date-time string.
   * 
   * @throws ParseException
   *           if the date could not be parsed.
   */

  private static final SimpleDateFormat[] s_formats = { new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"),
      new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss'Z'"), new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"),
      new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss") };

  private static final SimpleDateFormat[] s_formatsYMD = { new SimpleDateFormat("yyyy-MM-dd"),
      new SimpleDateFormat("yyyy-MM") };

  public static Date getDateFromISO8601(Object iso8601) throws ParseException {
    if (iso8601 == null)
      return null;
    String sdate = iso8601.toString();

    // "2009-12-04T23:29:06" -> 0 1 -> 2
    // "2009-12-04 23:29:06" -> 0 0 -> 3
    // "2009-12-04T23:29:06Z" -> 2 1 -> 0
    // "2009-12-04 23:29:06Z" -> 2 0 -> 1

    final int a = sdate.indexOf('Z') >= 0 ? 2 : 0;
    final int b = sdate.indexOf('T') >= 0 ? 1 : 0;
    SimpleDateFormat formatter = s_formats[3 - a - b];
    synchronized (formatter) {
      return formatter.parse(sdate);
    }
    // I did check this option too. It's slightly worse in terms of performance
    // and much worse in terms of memory consumption, thus synchronized option was chosen.
    // SimpleDateFormat is quite heavy guy to instantiate it every time, that's why.
    // And it's not thread safe. :(
    // return new SimpleDateFormat(s_formats[3 - a - b]).parse(sdate);
  }

  /**
   * 
   * FIX: In order to make EPL nested calls, we need this guy to be able to accept Object which is a string actually
   * 
   * @param rfc822
   *          An rfc 822 (section 5) date-time string.
   * 
   * @return the Unix epoch date-time for the given RFC 822 date-time string.
   * 
   * @throws ParseException
   *           if the date could not be parsed.
   */
  public static Date getDateFromRFC822String(Object rfc822) throws ParseException {
    return rfc822 == null ? null : new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").parse(rfc822.toString());
  }

  /**
   * Helper parsing two kinds of dates:<br>
   * 1. YYYY-MM-DD -> returns 00:00:00 (midnight) of this day <br>
   * 2. YYYY-MM -> returns 23:59:59 of THE LAST DAY of this month - this is for credit card expirations<br>
   * 
   * @param yyyymmdd
   * @return Date
   * @throws ParseException
   */
  public static Date getDateYYYYMMDD(Object yyyymmdd) throws ParseException {
    if (yyyymmdd == null)
      return null;
    String sdate = yyyymmdd.toString();
    if (sdate.lastIndexOf('-') > sdate.indexOf('-')) {
      SimpleDateFormat formatter = s_formatsYMD[0];
      synchronized (formatter) { // yes, it's better: see comment in getDateFromISO8601
        return formatter.parse(sdate);
      }
    }
    // otherwise we get the very last second credit card is valid at
    Calendar calendar = Calendar.getInstance();
    SimpleDateFormat formatter = s_formatsYMD[1];
    synchronized (formatter) { // yes, it's better: see comment in getDateFromISO8601
      calendar.setTime(formatter.parse(sdate));
    }
    int lastDay = calendar.getActualMaximum(Calendar.DATE);
    calendar.set(Calendar.DATE, lastDay);
    calendar.set(Calendar.HOUR_OF_DAY, 23);
    calendar.set(Calendar.MINUTE, 59);
    calendar.set(Calendar.SECOND, 59);
    return calendar.getTime();
  }

  /**
   * 
   * FIX: In order to make EPL nested calls like:
   * com.ebay.jetstream.util.DateUtil.getMillisFromISO8601(com.ebay.jetstream.epl
   * .EPLUtilities.getAttributeValue(attributes.values, 'marketplace.transaction_date')) possible, we need this guy to
   * be able to accept Object which is a string actually
   * 
   * 
   * @param iso8601
   *          an extended format ISO 8601 date-time string.
   * 
   * @return the Unix epoch date-time for the given ISO 8601 date-time string.
   * 
   * @throws ParseException
   *           if the date could not be parsed.
   */
  public static Long getMillisFromISO8601(Object iso8601) throws ParseException {
    Date iso8601Date = getDateFromISO8601(iso8601);
    return iso8601Date == null ? null : iso8601Date.getTime();
  }

  /**
   * 
   * FIX: In order to make EPL nested calls, we need this guy to be able to accept Object which is a string actually
   * 
   * @param rfc822
   *          An rfc 822 (section 5) date-time string.
   * 
   * @return the Unix epoch date-time for the given RFC 822 date-time string.
   * 
   * @throws ParseException
   *           if the date could not be parsed.
   */
  public static Long getMillisFromRFC822String(Object rfc822) throws ParseException {
    Date rfc822Date = getDateFromRFC822String(rfc822);
    return rfc822Date == null ? null : rfc822Date.getTime();
  }

  /**
   * Helper parsing two kinds of dates:<br>
   * 1. YYYY-MM-DD -> returns 00:00:00 (midnight) of this day <br>
   * 2. YYYY-MM -> returns 23:59:59 of THE LAST DAY of this month - this is for credit card expirations<br>
   * 
   * @param yyyymmdd
   * @return Long (milliseconds)
   * @throws ParseException
   */
  public static Long getMillisFromYYYYMMDD(Object yyyymmdd) throws ParseException {
    Date date = getDateYYYYMMDD(yyyymmdd);
    return date == null ? null : date.getTime();
  }

  /**
   * Returns current time in milliseconds for EPL
   * 
   * @return Long
   */
  public static Long getTimeInMillis() {
    Calendar calendar = Calendar.getInstance();
    return calendar.getTimeInMillis();
  }

  private DateUtil() {
  }
}
