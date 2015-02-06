/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.jetstream.notification.AlertListener;
import com.ebay.jetstream.notification.AlertListener.AlertStrength;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * vritual queue monitor - this monitor is used to monitor the growth of the Netty queues which are not accessible for outside
 * netty. 
 * 
 */

public class NettyVirtualQueueMonitor implements XSerializable, Externalizable {

	private static final long serialVersionUID = 1L;
	
	private AtomicLong m_queueBackLog = new AtomicLong(0);
	private AtomicLong m_maxQueueBackLog = new AtomicLong(0);
	private AtomicLong m_highWaterMark = new AtomicLong(0);
	private AtomicLong m_lowWaterMark = new AtomicLong(0);
	private AtomicLong m_queueOverflowTimeStamp = new AtomicLong(0);
	private AtomicBoolean m_queueFull = new AtomicBoolean();
	private AtomicLong m_overflowCount = new AtomicLong(0);
	private AtomicLong m_overflowCountThreshold = new AtomicLong(3);
	private AlertListener m_alertListener;
	private AtomicLong m_measurementInterval = new AtomicLong(6000L); 
	private String m_consumerHost = new String();
	

	public NettyVirtualQueueMonitor() {}

	/**
	 * @return
	 */
	public String getConsumerHost() {
		return m_consumerHost;
	}

	/**
	 * @param m_consumerHost
	 */
	public void setConsumerHost(String m_consumerHost) {
		this.m_consumerHost = m_consumerHost;
	}

	
	
	/**
	 * @param maxQueueBackLog
	 */
	public NettyVirtualQueueMonitor(long maxQueueBackLog) {
		m_maxQueueBackLog.set(maxQueueBackLog);
		m_highWaterMark.set((long) (maxQueueBackLog * 0.85));
		m_lowWaterMark.set((long) (maxQueueBackLog * 0.25));
	}

	/**
	 * 
	 */
	public void increment() {
		m_queueBackLog.incrementAndGet();
		if (m_queueBackLog.get() >= m_highWaterMark.get()) {
			m_queueFull.set(true);
			if (m_queueOverflowTimeStamp.get() == 0) {
				m_queueOverflowTimeStamp.set(System.currentTimeMillis());
				m_overflowCount.incrementAndGet();
			}
			else {
				if (System.currentTimeMillis() - m_queueOverflowTimeStamp.get() < m_measurementInterval.get()) {
					
										
					m_queueOverflowTimeStamp.set(System.currentTimeMillis());
					if (m_overflowCount.incrementAndGet() > m_overflowCountThreshold.get()) {
						// we should Alert now
						m_overflowCount.set(0);
						m_queueOverflowTimeStamp.set(0);
						if (m_alertListener != null) {
							m_alertListener.sendAlert("NettyEventProducer", "Slow consumer detected at host :" + getConsumerHost(), AlertStrength.RED);
						}
					}
				}
				else {
					m_queueOverflowTimeStamp.set(System.currentTimeMillis());
				}
			}
		}
	}

	/**
	 * 
	 */
	public void decrement() {
		m_queueBackLog.decrementAndGet();
		if (m_queueBackLog.get() <= m_lowWaterMark.get()) {
			m_queueFull.set(false);
		}
	}

	/**
	 * @return
	 */
	public boolean isQueueFull() {
		return m_queueFull.get();
	}

	/**
	 * @param maxQueueBackLog
	 */
	public void setMaxQueueBackLog(long maxQueueBackLog) {
		m_maxQueueBackLog.set(maxQueueBackLog);
		m_highWaterMark.set((long) (maxQueueBackLog * 0.85));
		m_lowWaterMark.set((long) (maxQueueBackLog * 0.25));
	}

	/**
	 * @return
	 */
	public long getQueueBackLog() {
		return m_queueBackLog.get();
	}

	public boolean isEmpty() {
		if (m_queueBackLog.get() <= 0)
			return true;
		else
			return false;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(m_queueBackLog.get());
		out.writeLong(m_maxQueueBackLog.get());
		out.writeLong(m_highWaterMark.get());
		out.writeLong(m_lowWaterMark.get());
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		m_queueBackLog.set(in.readLong());
		m_maxQueueBackLog.set(in.readLong());
		m_highWaterMark.set(in.readLong());
		m_lowWaterMark.set(in.readLong());
		
	}
	
	/**
	 * @return
	 */
	public AlertListener getAlertListener() {
		return m_alertListener;
	}

	/**
	 * @param alertListener
	 */
	public void setAlertListener(AlertListener alertListener) {
		this.m_alertListener = alertListener;
	}

	
	
	/**
	 * @return
	 */
	public long getOverflowCountThreshold() {
		return m_overflowCountThreshold.get();
	}

	
	/**
	 * @param overflowCountThreshold
	 */
	public void setOverflowCountThreshold(long overflowCountThreshold) {
		this.m_overflowCountThreshold.set(overflowCountThreshold);
	}

	
	
	/**
	 * @return
	 */
	public long getMeasurementInterval() {
		return m_measurementInterval.get();
	}

	
	
	/**
	 * @param measurementInterval
	 */
	public void setMeasurementInterval(long measurementInterval) {
		this.m_measurementInterval.set(measurementInterval);
	}

}
