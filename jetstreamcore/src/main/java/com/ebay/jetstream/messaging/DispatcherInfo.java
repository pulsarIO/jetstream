/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging;

import com.ebay.jetstream.messaging.interfaces.ITransportProvider;

/**
 * @author shmurthy
 *
 *
 */


public class DispatcherInfo {

	
	private ITransportProvider m_transport;
	private boolean m_dispatcherCreated;

	
	public ITransportProvider getTransport() {
		return m_transport;
	}

	public void setTransport(ITransportProvider m_transport) {
		this.m_transport = m_transport;
	}

	public boolean isDispatcherCreated() {
		return m_dispatcherCreated;
	}

	public void setDispatcherCreated(boolean m_dispatcherCreated) {
		this.m_dispatcherCreated = m_dispatcherCreated;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (m_dispatcherCreated ? 1231 : 1237);
		result = prime * result
				+ ((m_transport == null) ? 0 : m_transport.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof DispatcherInfo)) {
			return false;
		}
		DispatcherInfo other = (DispatcherInfo) obj;
		if (m_dispatcherCreated != other.m_dispatcherCreated) {
			return false;
		}
		if (m_transport == null) {
			if (other.m_transport != null) {
				return false;
			}
		} else if (!m_transport.equals(other.m_transport)) {
			return false;
		}
		return true;
	}
}
