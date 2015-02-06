/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.config;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 
 *
 */
public class ConfigDirectory extends ConfigFile {
	private FilenameFilter m_filter;
	private String[] m_items;

	/**
	 * @param theDirPath
	 */
	public ConfigDirectory(String theDirPath, FilenameFilter theFilter) {
		super(theDirPath);
		m_filter = theFilter;
	}

	public boolean isFolder() {
		return true;
	}

	public List<String> getStreamLocations() {
		populateItems();
		String root = getLocation();
		List<String> locations = new ArrayList<String>(m_items.length);
		for (int i = 0; i < m_items.length; i++)
			locations.add(root + "/" + m_items[i]);
		return Collections.unmodifiableList(locations);
	}

	public Iterator<ConfigStream> iterator() {
		populateItems();
		return new Iterator<ConfigStream>() {
			private int m_curr = 0;
			
			public boolean hasNext() {
				return m_items != null && m_curr < m_items.length;
			}

			public ConfigStream next() {
				return new ConfigFileStream(getLocation() + "/" + m_items[m_curr++]);
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	private void populateItems() {
		if (m_items == null)
			m_items = new File(getLocation()).list(m_filter);
	}
}
