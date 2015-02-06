/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * 
 *
 */
public class ConfigFile implements ConfigDataSource {
	private String m_filePath;

	public ConfigFile(String theFilePath) {
		m_filePath = theFilePath;
	}

	public boolean isFolder() {
		return false;
	}

	public String getLocation() {
		return m_filePath;
	}
	
	public List<String> getStreamLocations() {
		return Collections.unmodifiableList(new ArrayList<String>(0));
	}

	public Iterator<ConfigStream> iterator() {
		return new Iterator<ConfigStream>() {
			private ConfigStream m_stream = null;
			public boolean hasNext() { return m_stream == null; }
			public ConfigStream next() { return m_stream = new ConfigFileStream(m_filePath); }
			public void remove() { throw new UnsupportedOperationException(); }
		};
	}

	public String toString() {
		return super.toString() + " : " + getLocation();
	}
	
	/**
	 * Return a file input stream for a file path
	 */
	public static class ConfigFileStream implements ConfigDataSource.ConfigStream {
		private String m_location;
		
		public ConfigFileStream(String theLocation) {
			m_location = theLocation;
		}
		
		public String getLocation() {
			return m_location;
		}
		
		public InputStream getStream() throws IOException {
			return new FileInputStream(m_location);
		}
	}
}
