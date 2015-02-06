/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;


import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.event.channel.ChannelAddress;


/**
 * This class represents an "address" of the FileChannel (for both inbound and outbound), 
 * which consists of a file-path pointing to a local file containing DB records or 
 * JetStreamEvent, each record is a "line", i.e.
 * it is terminated by any one of a line feed ('\n'), a carriage return ('\r'),
 * or a carriage return followed immediately by a line feed. 
 * Each column/attribute in a record (line)
 * is separated by a user-specified special delimiter which should NOT be embedded in
 * the original data.
 * For example: CSV (Comma Separated Values) file where newline is used for 
 * separating the records and comma is used to separate the columns
 * the file looks like following:
 * 
 *   c11,c12,c13....\r\n
 *   c21,c22,c23....\r\n
 *   ...
 * The "address" also consists a list of column names. the number of columns in
 * the data file should match the number of column names.
 * In case there is no data for the column, the line can be like "c1,,c3...\r\n"
 * where c2 is missing because there is no value for this column.
 * 
 *                         
 * For how MySQL does load the data into a tabel from the a text file
 * see http://dev.mysql.com/doc/refman/5.0/en/load-data.html
 * 
 * @see CSVFileChannelAddress
 * 
 * @author gjin
 */

public class FileChannelAddress extends ChannelAddress {
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.channel.file.FileChannelAddress");
	private String m_pathname;         /* either relative path or full pathname of the DB data file---readable */
	private String[] m_columnNameArray= new String[0];  /* the fixed column names from callers */
	private String m_columnDelimiter=",";  /* delimiter between column. should not be embedded
	                                    original data */
	private Map<String, Object> m_columnNameToTypeMap; /*inbound only */
	
	/*** for outbound only ***/
	private long m_maxNumOfRecords = Long.MAX_VALUE;  /* no max, application's own risk */
	private boolean m_keepReservedKeys = false; /* should we remove those reserved keys from the events */
	
	public FileChannelAddress() {
		
	}

	
	public String getPathname() {
		return m_pathname;
	}
	public void setPathname(String path) {
		m_pathname = path;
		if (m_pathname == null || m_pathname.length() == 0) {
			throw new IllegalArgumentException(" filename is null or empty unexpectedly");
		}
		/* check readability */
		/**####
		File f = new File(m_pathname);
		if (!f.isFile() || !f.canRead()) {
			throw new IllegalArgumentException("Filename=" + m_pathname + " is not a file or not readable");
		}
		****/
		
	}
	public String getColumnDelimiter() {
		return m_columnDelimiter;
	}
	public void setColumnDelimiter(String columnDelimiter) {
		if (columnDelimiter == null || columnDelimiter.length() ==0 ) {
			throw new IllegalArgumentException("Column Delimiter is null or empty unexpectedly");
		}
		m_columnDelimiter = columnDelimiter;
	}
	
	public void setColumnNames(Map<String, Object> columnTypeMap) {
		/***###TODO: validation */
		validateCloumns(columnTypeMap);
		m_columnNameToTypeMap = Collections.unmodifiableMap(columnTypeMap);
		Set<String> keys = m_columnNameToTypeMap.keySet();
		m_columnNameArray = new String[keys.size()];
		keys.toArray(m_columnNameArray);
	}
	
	/**
	 * make sure the type is valid type and restore the value back with Class objects
	 * @param columnNameToTypeMap
	 */
	private void validateCloumns(Map<String, Object> columnNameToTypeMap) {
		if (columnNameToTypeMap == null) {
			throw new IllegalArgumentException("columnNameToTypeMap should not be null");
		}
		for (Map.Entry<String, Object> entry : columnNameToTypeMap.entrySet()) {
			Object objValue = entry.getValue();
			if (objValue == null) {
				throw new IllegalArgumentException("columnName=" + entry.getKey() + " has null type");
			}

			/* this should not happen */
			if (objValue instanceof Class<?>) {
				continue;
			}
			/* "java.lang.Long" or "java.lang.String", ...*/
			if (objValue instanceof String) {
				try {
					entry.setValue(Class.forName((String)objValue));
				} 
				catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("type field validation failed. e=", e);
				}
			}

		}
	}

	public Map<String, Object> getColumnNameToTypeMap() {
		return m_columnNameToTypeMap;
	}
	public String[] getColumnNameArray() {
		return m_columnNameArray;
	}
	
	
	/* for outbound only */
	public void setColumnNameArray(String[] columnNameArray) {
		if (columnNameArray == null) {
			throw new IllegalArgumentException("column name aray should not be null");
		}
		m_columnNameArray = new String[columnNameArray.length];
		StringBuffer sb = new StringBuffer();
		/* each column name should be non-null, non-empty string */
		for (int i=0; i<columnNameArray.length; i++) {
			m_columnNameArray[i] = columnNameArray[i];
			if (i != 0) {
				sb.append(",");
			}
				
			sb.append(columnNameArray[i]);
 			if (m_columnNameArray[i]== null || m_columnNameArray[i].length() ==0) {
				throw new IllegalArgumentException("the "+ i + "th column is either null or empty unexpectedly");
			}
		}
		LOGGER.warn( "filename=" + m_pathname + ", columnArray=" + sb.toString());
	}
	
	/* for outbound only */
	public void setMaxNumberOfRecords(long max) {
		if (max <=0) {
			return;
		}
		m_maxNumOfRecords = max;
	}
	public long getMaxNumberOfRecords() {
		return m_maxNumOfRecords;
	}
	public void setKeepReservedKeys(boolean keep) {
		m_keepReservedKeys = keep;
		
	}
	public boolean isKeepReservedKeys() {
		return m_keepReservedKeys;
	}
}
