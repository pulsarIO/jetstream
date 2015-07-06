/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	private static final String LOG_TS_FORMAT = "yyyyMMdd_HH:mm:ss";

	public static Date parseDate(String dateStr, String format)
			throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.parse(dateStr);
	}

	public static String formatDate(Date date, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
	}

	public static String formatDate(long dateTs, String format) {
		return formatDate(new Date(dateTs), format);
	}

	public static String formatDateForLog(long dateTs) {
		return formatDate(dateTs, LOG_TS_FORMAT);
	}

	public static Date parseDateFromLog(String dateStr) throws ParseException {
		return parseDate(dateStr, LOG_TS_FORMAT);
	}

}
