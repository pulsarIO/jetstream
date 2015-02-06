/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.AbstractOutboundChannel;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.notification.AlertListener;

/**
 * OutboundFileChannel can be used in place of or along with OutboundMessage Channel. 
 * @author gjin
 *
 */

@ManagedResource(objectName = "Event/Channel", description = "Outbound file component")
public class OutboundFileChannel extends AbstractOutboundChannel {
	
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");

	private Map<String, OutboundFileChannelHelper> m_streamTypeToHelperMap = new HashMap<String, OutboundFileChannelHelper>();
	private AlertListener m_AlertListener;
	
	private boolean m_stop = false;
	private boolean m_pause = false;
	private boolean m_restart = false;
	
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.event.channel.file");
	
	public OutboundFileChannel() {
	}
	public void setAlertListener(AlertListener al) {
		if (al == null) {
			LOGGER.error( "AlertListener is null unexpectedly");
			return;
		}
		m_AlertListener = al;
	}
	public void afterPropertiesSet() throws Exception {
		
	}
	
	
	public void setStreamTypeToAddressMap(Map<String, FileChannelAddress>  typeToAddressMap) {
		if (typeToAddressMap == null) {
			throw new IllegalArgumentException("typeToFileChannelAddress Map is null unexpectedly");
		}
		
		for (Entry<String, FileChannelAddress> entry:typeToAddressMap.entrySet()) {
			m_streamTypeToHelperMap.put(entry.getKey(), new OutboundFileChannelHelper(entry.getValue()));
		}
		
		
	}
	

	public void close() throws EventException {
		
		LOGGER.info( "Closing Outbound File Chanel");
		
		/***### do not remove the bean so we can do restart ***/
  		//Management.removeBeanOrFolder(getBeanName(), this);
  		
		// m_eventsReceivedPerSec.destroy();
		//m_eventsSentPerSec.destroy();
  	  
  		/* close the files for each stream */
  		for (String type:m_streamTypeToHelperMap.keySet() ) {
  			m_streamTypeToHelperMap.get(type).closeForWrite();
  		}

	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#flush()
	 */
	public void flush() throws EventException {
		// This is a NOOP for this channel
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.channel.ChannelOperations#getAddress()
	 */
	
	/* let us return the first address if there is any */
	public FileChannelAddress getAddress() {
		
		Set<String> types = m_streamTypeToHelperMap.keySet();
		if (types.size() == 0) {
			return null;
		}
		Iterator<String> iter = types.iterator();
		
		return m_streamTypeToHelperMap.get(iter.next()).getAddress();
	}
	

	
	    


	
	/**
	 * ChannelBinging.afterPropertiesSet() calls channel.open()
	 */

	public void open() throws EventException {
		
		LOGGER.info(  "Opening Outbound File Channel");
  		
  		/* put my name Event/FileChannel to the page */
		if (!m_restart) {			
			Management.addBean(getBeanName(), this);
		}
  	    m_eventsReceivedPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
  		m_eventsSentPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
  	  

  		/* open the event file, be ready for writing*/
  		/* close the files for each stream */
  		
  		for (String type:m_streamTypeToHelperMap.keySet() ) {
  			try { 
  				m_streamTypeToHelperMap.get(type).openForWrite();
  			} catch (EventException ee) {
  				LOGGER.error( "open() get e=" + ee.getLocalizedMessage(), ee);
  	  	  		/*** instead of forcing application to exit, we choose to post an error status at dashboard */
  				if (m_AlertListener != null) {
  					m_AlertListener.sendAlert(getBeanName(),  "OutboundFileChannel open() get e=" + ee, AlertListener.AlertStrength.RED);
  				} else {
  					throw ee;
  				}
  			}
  		}

	}

		
	
	/**
	 * 
	 */
	@ManagedOperation
	public void resetStats() {
		super.resetStats();
	}


	/* create a line "v1,v2,,v4,v5\n" from the event
	 * if a value for a key is null then ",,". in the case above, v3 is null 
	 */
	private String createEventLine(JetstreamEvent e, String[] keys, String delimiter) {
		StringBuffer sb  = new StringBuffer();
		boolean isFirst = true;
		for (String key: keys) {
			if (isFirst) {
				isFirst = false;
			} else {
				sb.append(delimiter);
			}
			if (e.get(key) != null) {
				sb.append(e.get(key));
			}
		}
		sb.append(LINE_SEPARATOR);
		return sb.toString();
	}
	
	/*** this is the point where the data get into this class */
	public void sendEvent(JetstreamEvent event) throws EventException {
		if (m_stop || m_pause) {
			return;
		}
 		if (event == null) {
			return;
		}
		incrementEventRecievedCounter();
		
		String streamType = event.getEventType();
		
		if (!m_streamTypeToHelperMap.containsKey(streamType)) {
			/***#### INFO Logging here */
			//LOGGER.info(   "the stream type=" + streamType + " is not registered", getBeanName());
			incrementEventDroppedCounter();
			return;
		}
		
			
		OutboundFileChannelHelper helper = m_streamTypeToHelperMap.get(streamType);
		FileChannelAddress address = helper.getAddress();
		
		JetstreamEvent userEvent = null;
		
		/* remove those reserved keys (such as js_ev_type...which were added by jetstream) from the event */
		if (!address.isKeepReservedKeys()){
			userEvent = event.getFilteredEvent();
		} else {
			userEvent = new JetstreamEvent();
			userEvent.putAll(event);
			/* but should we still need to remove affinity key--- no
			userEvent.remove(JetstreamReservedKeys.MessageAffinityKey.toString());
			***/
		}
		
		/** empty event. drop it */
		if (userEvent.size() == 0) {
			LOGGER.info(   "the event is empty. Should not happen?");
			incrementEventDroppedCounter();
			return;
		}
		
		/*v1,v2,,,,,\r\n */
		String eventLineString = null;
		
		/* now the event must have at least one key 
		 * If columnname array is not predefined.
		 * it seems the first event of this stream. then we generate the keys from it and assume
		 * that all events of this stream have the same set of keys.
		 * */
		if (address.getColumnNameArray().length== 0) {
			synchronized (this) {
				if (address.getColumnNameArray().length== 0) {
					Set<String> keys = userEvent.keySet();
					
					String[] keyArray = new String[keys.size()];
					keys.toArray(keyArray);
					address.setColumnNameArray(keyArray);
				}
			}

		} 
		/* now we have the column info */
		eventLineString = createEventLine(userEvent, address.getColumnNameArray(), address.getColumnDelimiter());

		if (helper.writeEvent(eventLineString)) {
			incrementEventSentCounter();
		} else {
			incrementEventDroppedCounter();
		}

	}


	public void shutDown() {
		close();
	}


	@Override
	public String toString() {

		return getBeanName();
	}

		

  	@Override
  	public void processApplicationEvent(ApplicationEvent event) {
  		/* we are not going to support for the time being */
  	}

	@Override
	public int getPendingEvents() {
		
		return 0;
	}
	@ManagedOperation
	public void stop() {
		m_stop = true;
		
		/* close all down load stream so we can detach the files from */
		close();
	}
	
	@ManagedOperation
	public void pause() {
		m_pause = true;
	}
	@ManagedOperation
	public void resume() {
		m_pause = false;
	}
	
	/* restart() to allow download more data if download files have more space */
	@ManagedOperation
	public void restart() {
		if (!m_stop) {
			return;
		}
		m_restart = true;
		open();
		m_restart = false;
		m_stop = false;
		m_pause = false;
	}
	
}
