/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * 
 * 
 * Provide a generic way of iterating over and retrieving configuration data.
 */
public interface ConfigDataSource extends Iterable<ConfigDataSource.ConfigStream> {
	boolean isFolder() throws IOException;
	String getLocation();
	List<String> getStreamLocations() throws IOException;
	interface ConfigStream {
		String getLocation();
		InputStream getStream() throws IOException;
	}
}
