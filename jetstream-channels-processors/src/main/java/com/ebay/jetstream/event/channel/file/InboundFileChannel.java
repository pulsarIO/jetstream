/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;


import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.counter.LongCounter;
import com.ebay.jetstream.counter.LongEWMACounter;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.RetryEventCode;
import com.ebay.jetstream.event.advice.Advice;
import com.ebay.jetstream.event.channel.InboundChannel;
import com.ebay.jetstream.event.support.AbstractEventSource;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageServiceTimer;
import com.ebay.jetstream.notification.AlertListener;
import com.ebay.jetstream.xmlser.XSerializable;

/***
 * specify this class in the subscriber's wiring file to get registered into Spring framework
 * @author gjin
 *
 */

/**
 * with this annotation will see http://127.0.0.1:1504/Event/Channel/InboundFileChannel
 * if the bean name is "InboundFileChannel" 
 */
@ManagedResource(objectName = "Event/Channel", description = "Inbound File component")
public class InboundFileChannel extends AbstractEventSource implements InboundChannel, BeanNameAware,
	               InitializingBean, ApplicationListener, XSerializable, ShutDownable {

	Logger logger = LoggerFactory.getLogger("com.ebay.jetstream.event.channel.file.InboundFileChannel");
	
	//private FileChannelAddress m_address;
	private InboundFileChannelHelper m_fcHelper;
  
	private Advice m_adviceListener;

	private final AtomicBoolean m_isPaused = new AtomicBoolean(false);

	private final LongCounter m_totalEventsReceived = new LongCounter();

	private final LongCounter m_pauseCount = new LongCounter();

	private final LongCounter m_resumeCount = new LongCounter();

	private final LongCounter m_totalEventsSent = new LongCounter();
  
	private final LongCounter m_totalEventsDropped = new LongCounter();

	private final LongCounter m_eventSentToAdviceListener = new LongCounter();
  
	private LongEWMACounter m_eventsReceivedPerSec;
	private LongEWMACounter m_eventsSentPerSec;

	private final AtomicBoolean m_shutdownStatus = new AtomicBoolean(false); // NOPMD
  
	private int m_numOfEventsPerSec;  /* the trunkSize: number of events/per sec */

	private PlayThread m_pt = new PlayThread();
	
	private AlertListener m_AlertListener;

	//private AtomicBoolean m_restartable = new AtomicBoolean(false);
  
	public InboundFileChannel() {
	}

	public void afterPropertiesSet() throws Exception {
   
	}

	public void setAlertListener(AlertListener al) {
		if (al == null) {
			logger.error( "AlertListener is null unexpectedly");
			return;
		}
		m_AlertListener = al;
	}
	/**
	 * clean the memory
	 * close the file
	 */
  	public void close() throws EventException {
  		logger.info( "Closing Inbound File Channel");
   		Management.removeBeanOrFolder(getBeanName(), this);
  		
  	    m_eventsReceivedPerSec.destroy();
  		m_eventsSentPerSec.destroy();
  	  
  		/* close the file */
  		m_fcHelper.closeForRead();
  		
  	}
  	
  	public void open() throws EventException {
  		//LOGGER.info( "Opening Inbound File Channel");
  		logger.info( "Openning Inbound File Channel");
  		/* put my name Event/FileChannel to the page */
  		Management.addBean(getBeanName(), this);
  	    m_eventsReceivedPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
  		m_eventsSentPerSec = new LongEWMACounter(60, MessageServiceTimer.sInstance().getTimer());
  	  

  		/* open the event file, be ready for play*/
  		try {
  			m_fcHelper.openForRead();
  		} catch (EventException ee) {
  			//LOGGER.error( "InboundFileChannel open() get e=" + ee);
  			logger.error( "--InboundFileChannel open() get e=" + ee);
  	  		/*** instead of forcing application to exit, we choose to post an error status at dashboard */
			if (m_AlertListener != null) {
				m_AlertListener.sendAlert(getBeanName(),  "InboundFileChannel open() get e=" + ee, AlertListener.AlertStrength.RED);
			} else {
				throw ee;
			}
				
  		}


  	}
  	
  	private void fireEvent(JetstreamEvent evt) {
  		super.fireSendEvent(evt);
  	}

  	/* just for satisfying the interface */
  	public void flush() throws EventException {
  	}
 
  	@Override
  	public int getPendingEvents() {
  		if (m_fcHelper.isEOF()) {
  			return 0;
  		} else {
  			return 1; /* we do not know how many left */
  		}
  	}
  	
  	public FileChannelAddress getAddress() {
  		return m_fcHelper.getAddress();
  	}

  	public Advice getAdviceListener() {
  		return m_adviceListener;
  	}



  	/**
  	 * 
  	 * @return
  	 */
  	public long getEventSentToAdviceListener() {
  		return m_eventSentToAdviceListener.get();
  	}

  	/**
  	 * @return the messagesReceived
  	 */
  	public long getTotalEventsReceived() {
  		return m_totalEventsReceived.get();
  	}



  	/**
  	 * @return the pauseCount
  	 */
  	public long getTotalPauseCount() {
  		return m_pauseCount.get();
  	}




  	/**
  	 * @return the resumeCount
  	 */
  	public long getTotalResumeCount() {
  		return m_resumeCount.get();
  	}

  
  	public boolean getShutdownStatus() {
  		return m_shutdownStatus.get();
  	}

  	private void incrementAdviceListenerCount() {
  		m_eventSentToAdviceListener.increment();
  	}

  	private void incrementSentCount() {
  		m_totalEventsSent.increment();
  	}
  	
  	

  	@Override
  	public long getEventsReceivedPerSec() {
  		return m_eventsReceivedPerSec.get();
  	}

  	@Override
  	public long getEventsSentPerSec() {
  		return m_eventsSentPerSec.get();
  	} 

	@ManagedOperation
	public void startReplayWithEventFile() {
		Thread.State s = m_pt.getState();
		/* if it is newly created or terminated, then start it */
		if (s == Thread.State.NEW || s == Thread.State.TERMINATED) {
			m_pt.start();
		} else {
	  		logger.warn( "the replay thread was started with state=" + s);
		}
	}

	@ManagedOperation
	public void restart() {
		Thread.State s = m_pt.getState();
		/* if it is newly created or terminated, then start it */
		if (s == Thread.State.TERMINATED) {
	  		/* open the event file */
	  		m_fcHelper.openForRead();
	  		/* restart play from the begin of the event file */
	  		m_pt = new PlayThread();
			m_pt.start();
		} else {
			logger.warn( "the replay thread either never started or running: state=" + s);
		}
	}
  
  	@Override
  	@ManagedOperation
  	public void pause() throws EventException {
  		if (m_isPaused.get()) {
  			logger.error( "InboundFileChannel " + getBeanName()
  					+ " could not be paused. It is already in paused state", "InboundFileChannel" + ".pause()");
  			return;
  		}
  		m_isPaused.set(true);
  		
  		logger.error( "Inbound File Channel paused.");

  	}
  	@Override
  	@ManagedOperation
  	public void resume() throws EventException {
  		if (!m_isPaused.get()) {
  			logger.error( "InboundFileChannel " + getBeanName()
  					+ " could not be resumed. It is already in resume state", "InboundFileChannel" + ".pause()");
  			return;
  		}

  		// if we are in paused state then first empty the pause Cache which could have received events
  		// that were in the pipeline when we unsubscribed in response to a pause.
  		if (m_isPaused.get() && !m_shutdownStatus.get()) {

  			m_isPaused.set(false);
  			if (m_resumeCount.addAndGet(1) < 0) {
  				m_resumeCount.set(0);
  			}
  			
  			/* resume by interrupt the thread from "sleep()" */
  			m_pt.interrupt();
  		}

  	}

  	@Override
  	protected void processApplicationEvent(ApplicationEvent event) {
  		if (event instanceof ContextBeanChangedEvent) {
  			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;

  			//Calculate changes
  			if (bcInfo.isChangedBean(getAddress())) {
  				logger.info( "Received new configuration for InboundMessageChannel - " + getBeanName());
  				try {
  					close();
  				}
  				catch (Throwable e) {
  					//swallow the exception and log
  					String errMsg = new String("Error closing InboundMessagingChannel while applying new config - ");
  					errMsg += e.getMessage();

  					logger.error(  errMsg);

  				}
  				setAddress((FileChannelAddress) bcInfo.getChangedBean());
  				try {
  					open();
  				}
  				catch (Throwable e) {
  					// swallow the exception and log
  					String errMsg = new String("Error opening InboundMessagingChannel while applying new config - ");
  					errMsg += e.getMessage();

  					logger.error(  errMsg);

  				}
  			}
  		}

  	}

  	/**
  	 * @param messagesReceived
  	 *          the messagesReceived to set
  	 */
  	@ManagedOperation
  	public void resetEventsReceived() {
  		m_totalEventsReceived.set(0);
  	}

  	/**
  	 * @param pauseCount
  	 *          the pauseCount to set
  	 */	
  	@ManagedOperation
  	public void resetPauseCount() {
  		m_pauseCount.set(0);
  	}

  	/**
  	 * @param resumeCount
  	 *          the resumeCount to set
  	 */
  	@ManagedOperation
  	public void resetResumeCount() {
  		m_resumeCount.set(0);
  	}

  	/**
  	 * 
  	 */
  	@ManagedOperation
  	public void resetStats() {
  		m_totalEventsReceived.reset();
   		m_pauseCount.reset();
  		m_resumeCount.reset();
  		m_totalEventsSent.reset();
   		m_totalEventsDropped.reset();
   		
   	    m_totalEventsReceived.reset();
   	    m_eventsReceivedPerSec.reset();
  	}



  	private void sendToAdviceListener(JetstreamEvent event, RetryEventCode code, String msg) {
  		if (m_adviceListener != null) {
		  	m_adviceListener.retry(event, code, msg);
      		incrementAdviceListenerCount();
    	}
  	}

  	/**
  	 * @param address
  	 */
  	public void setAddress(FileChannelAddress address) {
	  	//m_address = (FileChannelAddress) address;
	  	m_fcHelper = new InboundFileChannelHelper(address);
  	}

  	public void setAdviceListener(Advice adviceListener) {
	  	m_adviceListener = adviceListener;
  	}

  
  	@Override
  	public void shutDown() {
  		m_shutdownStatus.set(true);
  		close();
  	}


  	@Override
  	public long getTotalEventsSent() {
  		return m_totalEventsSent.get();
  	}

  	@Override
	public long getTotalEventsDropped() {
		return m_totalEventsDropped.get();
	}





	public int getNumOfEventPerSec() {
		return m_numOfEventsPerSec;
	}
	public void setNumOfEventPerSec(int rate) {
		m_numOfEventsPerSec = rate;
	}
	
	

	class PlayThread extends Thread {
		Map<String, Object> eventMap;
		public void run() {
			//System.out.println("play thread starts to run: eof=" + m_fcHelper.isEOF() + ", paused=" + m_isPaused.get()); //KEEPME
			while (!m_fcHelper.isEOF()) {
				while (m_isPaused.get()) {
					try {
						Thread.sleep(1000*10);
					} catch (InterruptedException ie) {
						System.out.println("##### innterrupted "); //KEEPME
					}
				}
				long startTimeMS = System.currentTimeMillis();
				
				int count;
				//System.out.println("##### m_numOfEventPerSec=" + m_numOfEventsPerSec); //KEEPME
				for (count = 0; count < m_numOfEventsPerSec; count++) {
					//System.out.println("##### getNextEventMap()");
					eventMap = m_fcHelper.getNextEventMap();
 
					if (eventMap == null) {
						break;
					}
					JetstreamEvent event = extractEventFromMap(eventMap);
					if (event != null) {
						m_totalEventsReceived.increment();
						m_eventsReceivedPerSec.increment();
						fireSendEvent(event);
					}
				}
				
				long elapsedTimeInMS = System.currentTimeMillis() - startTimeMS;
				
				/* if less than one sec */
				if (elapsedTimeInMS < 1000) {

					try {
						//System.out.println("##### sleep " + (1000 - elapsedTimeInMS) + " millisec"); //KEEPME
						TimeUnit.MILLISECONDS.sleep(1000 - elapsedTimeInMS);
					} catch (InterruptedException e) {
						// swallow
					}
				}

			}
			/* end of file. close it for restart */
			m_fcHelper.closeForRead();
		}
		private JetstreamEvent extractEventFromMap(Map<String, Object> eventMap) {
			JetstreamEvent event;
			if (eventMap == null) {
				return null;
			}
			
			
			String type = (String) eventMap.get(JetstreamReservedKeys.EventType.toString());
			
			eventMap.remove(JetstreamReservedKeys.EventId.toString());
			eventMap.remove(JetstreamReservedKeys.EventType.toString());
			
			/* TODO: it appears we do not have any class implementing EventId 
			 * so we put null for the time being
			 */
			event = new JetstreamEvent();
			if (type != null) {
				event.setEventType(type);
			}
			
			for (Map.Entry<String, Object> entry: eventMap.entrySet()) {
				event.put(entry.getKey(), entry.getValue());
			}
			return event;
		}
	}
}
