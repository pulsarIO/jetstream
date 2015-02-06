/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.generator;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.JetstreamReservedKeys;
import com.ebay.jetstream.event.channel.AbstractInboundChannel;
import com.ebay.jetstream.event.channel.ChannelAddress;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.XSerializable;

@ManagedResource(objectName = "Event/EventGenerator", description = "Generic Event Generator")
public class GenericEventGenerator extends AbstractInboundChannel implements
		InitializingBean, XSerializable {

	private EventBuilder m_bldr;
	private boolean m_bSendEventsOnInit;
	private boolean genAfinityKey=true;
	private int numExtraFields = 0;
	private AtomicInteger rate = new AtomicInteger(100);
	private long m_nEventCount;
	private AtomicBoolean running = new AtomicBoolean(false);
	private volatile Thread m_runner;
	private AtomicBoolean m_pause = new AtomicBoolean(false);
	private Object m_monitor = new Object();

	@Override
	public void afterPropertiesSet() throws Exception {
		Management.addBean(getBeanName(), this);

		if (m_bldr == null)
			m_bldr = new InternalEventBuilder();

		if (m_bldr instanceof InternalEventBuilder) {
			((InternalEventBuilder) m_bldr).setExtraFieldsCount(numExtraFields);
			((InternalEventBuilder) m_bldr).setGenAfinityKey(genAfinityKey);
		}
		
		

		m_bldr.initialize();
	}

	public long getEventCount() {
		return m_nEventCount;
	}

	@Override
	@ManagedOperation
	public void pause() {
		m_pause.set(true);
	}

	@Override
	@ManagedOperation
	public void resume() {
		m_pause.set(false);
		synchronized(m_monitor) {
			m_monitor.notifyAll();
		}
	}

	@ManagedOperation
	public void IncrementRateBy1000() {
		rate.addAndGet(1000);
	}

	@ManagedOperation
	public void DecrementRateBy1000() {
		if (rate.get() > 1000) {
			int newrate = rate.get() - 1000;
			rate.set(newrate);
		}
	}

	public void setEventBuilder(EventBuilder bldr) {
		m_bldr = bldr;
	}

	public void setEventCount(long nEventCount) {
		m_nEventCount = nEventCount;
	}

	public void setGenAfinityKey(boolean genAfinityKey) {
		this.genAfinityKey = genAfinityKey;
	}

	public void setNumExtraFields(int numExtraFields) {
		this.numExtraFields = numExtraFields;
	}

	public void setRate(int rate) {
		System.out.println(rate);
		this.rate.set(rate);
	}

	public int getRate() {
		return rate.get();
	}

	public void setSendEventsOnInit(boolean bSendOnInit) {
		m_bSendEventsOnInit = bSendOnInit;
	}

	@ManagedOperation
	public void startReplay() {

		if (running.get()) {
			System.out.println("Already running");
			return;
		}
		m_runner = new Thread(new Runnable() {
			public void run() {

				try {
					long sendCount = 0;

					running.set(true);
					
					while (sendCount < m_nEventCount) {
						
						if (m_pause.get()) {
							synchronized(m_monitor) {
								try {
									m_monitor.wait(100000);
								} catch (InterruptedException ie) {

								}
								continue;
							}
						}
							
						long startTime = System.currentTimeMillis();
						int temprate = rate.get();
						for (int count = 0; count < temprate; count++) {

							JetstreamEvent event = new JetstreamEvent();
							event.setEventType(m_bldr.getEventStreamName());
							
							m_bldr.populateEvent(event);

							fireSendEvent(event);

							if (++sendCount > m_nEventCount)
								break;
						}

						long diffTime = System.currentTimeMillis() - startTime;
						// System.out.println("difftime = " + diffTime +
						// " - Thread ID = " + Thread.currentThread().getId());

						if (diffTime < 1000) {

							try {
								TimeUnit.MILLISECONDS.sleep(1000 - diffTime);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}

					running.set(false);
					m_runner = null;

				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
			}
		});
		m_runner.start();

	}

	@Override
	protected void processApplicationEvent(ApplicationEvent arg0) {
		if (m_bSendEventsOnInit && arg0 instanceof ContextStartedEvent)
			startReplay();
	}

	public interface EventBuilder {

		String getEventStreamName();

		String getPoolOfOrigin();

		void initialize();

		JetstreamEvent populateEvent(JetstreamEvent event);
	}

	public static class InternalEventBuilder implements EventBuilder {

		private final Random m_r = new SecureRandom();
		private int m_numExtraFields;
		private boolean m_bGenAffinity;
		private String[] key_array = { "id", "name", "age" };
	
		public InternalEventBuilder() {
			// for spring
		}

		public void setGenAfinityKey(boolean bGenAffinity) {
			m_bGenAffinity = bGenAffinity;
		}

		public void setExtraFieldsCount(int nExtraFields) {
			m_numExtraFields = nExtraFields;
		}

		@Override
		public void initialize() {

		}

		@Override
		public JetstreamEvent populateEvent(JetstreamEvent event) {

			event.put(key_array[0], "id_" + m_r.nextLong());
			event.put(key_array[1], "name_" + m_r.nextLong());
			event.put(key_array[2], m_r.nextInt(100));

			for (int i = 0; i < m_numExtraFields; i++)
				event.put("field_" + Long.toString(i), Math.abs(m_r.nextLong()));

			if (m_bGenAffinity)
				event.put(JetstreamReservedKeys.MessageAffinityKey.toString(),
						event.get("age"));

			return event;
		}

		@Override
		public String getEventStreamName() {
			return "Person";
		}

		@Override
		public String getPoolOfOrigin() {
			return "Sample Event";
		}

	}

	@Override
	public String getBeanName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush() throws EventException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ChannelAddress getAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPendingEvents() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void shutDown() {
		// TODO Auto-generated method stub
		
	}
}
