/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;
/**
 * if column separator is not a comma, please
 * use FileChannelAddress directly.
 * 
 * @author gjin
 *
 */
public class CSVFileChannelAddress extends FileChannelAddress {

	public CSVFileChannelAddress() {
		super();
		setColumnDelimiter(",");
	}
}
