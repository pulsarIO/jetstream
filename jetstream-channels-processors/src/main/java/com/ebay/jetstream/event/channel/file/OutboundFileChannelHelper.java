/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.event.EventException;

/**
 * This class was and should be part of FileChannelAddress or a base class of OutboudFileChannel
 * if we follow "expert" pattern. However,
 * to make the "address" simple, we move some implementation 
 * details from the "address" class into this "helper" class.
 * 
 * Note: one outboundfilechannelAddress is 1-1 mapping to one such helper!
 * 
 * This class will not do any sanity checking which is performed 
 * by the "address" class already.
 * 
 * @author gjin
 */

public class OutboundFileChannelHelper {
	private FileChannelAddress m_address;
	
	private FileOutputStream m_fos = null; 
	private AtomicLong m_totalWrites = new AtomicLong();
	private AtomicBoolean m_reportFull = new AtomicBoolean();
	
	private AtomicLong m_errorLoggingRate = new AtomicLong(5000); /* log every 5000 errors initially*/
	private AtomicLong m_numberOfErrors = new AtomicLong();
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.channel.file");

	
	/***
	 * 
	 */
	public OutboundFileChannelHelper(FileChannelAddress address) {
		m_address = address;
		if (m_address == null) {
			throw new IllegalArgumentException("the channel address must be no null");
			
		}
	
	}
	
	public FileChannelAddress getAddress() {
		return m_address;
	}
	
	
	
	public void closeForWrite() throws EventException {
		if (m_fos == null) {
			LOGGER.info( "try to close an event file " + m_address.getPathname() + " which was not open", "CloseUnOpenFile");
			return;  /* close already or never open*/
		}
		try {
			m_fos.flush();
			m_fos.close();
		} catch (IOException e) {
			throw new EventException("close file: " + m_address.getPathname(), e);
		}
		m_fos = null;
	}
	
	
	/***
	 * open the file with appending mode. if the file exists already, a warning log is issued.
	 * if failed to create or open the file, a error log is issued.
	 */
	public void openForWrite() throws EventException {
		File f = new File(ConfigUtils.getInitialPropertyExpanded(m_address.getPathname()));
		try  {
			if (!f.exists()) {
				f.createNewFile();
			} else {
				LOGGER.info( "appending to file=" + m_address.getPathname());
			}
		} catch (IOException ioe) {
			throw new EventException("failed to create file=" + m_address.getPathname() + ", e=" + ioe);
			
		}
		try  {
			/*** append mode ***/
			m_fos = new FileOutputStream(f, true);
		} catch (FileNotFoundException fnfe) {
			throw new EventException("failed to open file=" + m_address.getPathname() + ", e=" + fnfe);
		}
	
	}
	
	/**
	 * if the file is full, we report the WARNING and close the file
	 * @param lineEvent
	 * @return true if write is a success
	 */
	public boolean writeEvent(String lineEvent) {
		if (m_fos == null) {
			return false;
		}
		if (lineEvent == null ) {
			return false;
		}
		if (m_totalWrites.get() >= m_address.getMaxNumberOfRecords()) {
			/* should we not sync here to save some performance. as a result we may see a few similar errors at the same time */
			if (!m_reportFull.getAndSet(true)) { 
				LOGGER.warn( "file=" + m_address.getPathname() + " reaches the max number of records =" + 
			                                m_address.getMaxNumberOfRecords() + " in this session, no more write!");
				closeForWrite();
			}
			return false;
		}
		
		try {
			m_fos.write(lineEvent.getBytes());
			m_fos.flush();
		} catch (IOException e) {
			if (m_numberOfErrors.getAndIncrement() % m_errorLoggingRate.get() ==0) {
				LOGGER.error( "failed to write file=" + m_address.getPathname() + ", e=" + e);
			}
			return false;
		}
		m_totalWrites.getAndIncrement();
		//System.out.println("id=" + Thread.currentThread().getId() + ", total=" + m_totalWrites.get());
		return true;
		
	}
	
}
