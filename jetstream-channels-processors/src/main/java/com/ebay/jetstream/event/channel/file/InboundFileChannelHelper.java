/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.event.EventException;


/**
 * This class was and should be part of FileChannelAddress or a base class of InbounfFileChannel
 * if we follow "expert" pattern. However,
 * to make the "address" simple, we move some implementation 
 * details from the "address" class into this "helper" class.
 *  
 * This class will not do any sanity checking which is performed 
 * by the "address" class already.
 * 
 * @author gjin
 */

public class InboundFileChannelHelper {
	private FileChannelAddress m_address;

	private BufferedReader m_br;   /*inbound only:  the reader associated with the local file */
	
	private boolean m_eof = false; /*inbound only */
	
	private FileOutputStream m_fos = null; /* for outbound */
	private AtomicLong m_totalWrites = new AtomicLong();
	private AtomicBoolean m_reportFull = new AtomicBoolean();
	
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.channel.file");
	private AtomicLong m_errorLoggingRate = new AtomicLong(5000); /* log every 5000 errors initially*/
	private AtomicLong m_numberOfErrors = new AtomicLong();

	
	/***
	 * 
	 */
	public InboundFileChannelHelper(FileChannelAddress address) {
		m_address = address;
		if (m_address == null) {
			throw new IllegalArgumentException("the channel address must be no null");
			
		}
	
	}
	
	public FileChannelAddress getAddress() {
		return m_address;
	}
	
	/* open the file */
	public void openForRead() throws EventException {
		if (m_br != null) {
			LOGGER.info( "try to open an event file " + m_address.getPathname() + " which was open", "OpenFileTwice");
			return;  /* open already */
		}
		File f = new File(ConfigUtils.getInitialPropertyExpanded(m_address.getPathname()));
		
		if (!f.isFile() || !f.canRead()) {
			throw new EventException("Filename=" + m_address.getPathname() + " is not a file or not readable");
		}
		try {
			m_br = new BufferedReader(new InputStreamReader(new FileInputStream(m_address.getPathname())));
		} catch (FileNotFoundException e) {
			throw new EventException("open file: " + m_address.getPathname(), e);
		}
		m_eof= false;
	}
	
	/**inbound only */
	public void closeForRead() throws EventException {
		if (m_br == null) {
			LOGGER.info( "try to close an event file " + m_address.getPathname() + " which was not open", "CloseUnOpenFile");
			return;  /* close already or never open*/
		}
		try {
			m_br.close();
		} catch (IOException e) {
			throw new EventException("close file: " + m_address.getPathname(), e);
		}
		m_br = null;
	}
	
	

	
	
	/***
	 * inbound
	 * @return null when file is not open, or running into IO exception, EOF, bad records
	 */
	public Map<String, Object> getNextEventMap() {
		/***TODO: what is event type, ID?
		 * 
		 */
		HashMap<String, Object> event = new HashMap<String, Object>();
		if (m_br == null) {
			if (m_numberOfErrors.getAndIncrement() % m_errorLoggingRate.get() ==0) {
				LOGGER.error( "the event file is not open");
			}
			return null;
		}
		if (m_eof) {
			if (m_numberOfErrors.getAndIncrement() % m_errorLoggingRate.get() ==0) {
				LOGGER.error( "the event file has reached the end");
			}
			return null;
		}
		String line;
		try {
			line = m_br.readLine();
		} catch (IOException e) {
			/* report ERROR */
			LOGGER.error( e.getLocalizedMessage(), e);
			return null;
		}
		if (line== null) {
			/* report eof--- not an error */
			LOGGER.warn( "file reaches the end");
			m_eof = true;
			return null;
		}
		if (parseEvent(event, line)) {
			return event;
		} else {
			return null;
		}
		
		
	}
	
	private boolean parseEvent(Map<String, Object> event, String s) {
		Long longObj = 0L;
		Integer intObj = 0;
		Float  floatObj = 0.0f;
		String[] columns = s.split(m_address.getColumnDelimiter(), -1);
		
		if (columns.length != m_address.getColumnNameArray().length) {
			/* the data has either more or less columns */ 
			LOGGER.error( "the number of columns in a record!=the number of cloumns:" +
					   " data has "	+ columns.length + ", expected " + m_address.getColumnNameArray().length);
			return false;
		}
		
		/* build map <columname, value> i.e.
		 * {<c1, v1>, <c2, v2>...} for a data record 
		 */
		for (int i=0; i< columns.length; i++) {
			String key = m_address.getColumnNameArray()[i];
			Class xlass = (Class) (m_address.getColumnNameToTypeMap().get(key));
			
			//Object xlass = Class.forName((String) (m_address.getColumnNameToTypeMap().get(key)));
			
			
			/****TODO ****/
			if (xlass.isInstance(longObj)) {
				
				long value; 
				try {
					value = Long.parseLong(columns[i]);
				} catch (NumberFormatException e) {
					LOGGER.info( "incorrect long value=" + columns[i]);
				    return false;
				}
				event.put(m_address.getColumnNameArray()[i], value);
			} else if (xlass.isInstance(intObj)) {
				int value;
				try {
					value = Integer.parseInt(columns[i]);
				} catch (NumberFormatException e) {
					LOGGER.info( "incorrect int value=" + columns[i]);
				    return false;
				}
				
				event.put(m_address.getColumnNameArray()[i], value);
			}else if (xlass.isInstance(floatObj)) {
				float value;
				try {
					value = Float.parseFloat(columns[i]);
				}catch (NumberFormatException e) {
					LOGGER.info( "incorrect float value=" + columns[i]);
				    return false;
				}
				event.put(m_address.getColumnNameArray()[i], value);
			} else { 
				event.put(m_address.getColumnNameArray()[i], columns[i]);
			}
		}
		return true;
	}
	
	public boolean isEOF() {
		return m_eof;
	}
	
	
}
