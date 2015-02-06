/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.exception;

/**
 * @author shmurthy (shmurthy@ebay.com)
 * 
 *  Exception class for Message Service
 */
public class MessageServiceException extends Exception {

	private static final long serialVersionUID = 1L;

	public final static int BUFFER_FULL = 1;
	public final static int TRANSPORT_ERROR = 2;
	public final static int SERVICE_UNINITIALIZED = 3;
	public final static int NO_TRANSPORTS_INSTALLED = 4;
	public final static int INITIALIZATION_ERROR = 5;
	public final static int SHUTDOWN_FAILED = 6;
	public final static int UNSUPPORTED_MSG_PRIORITY = 7;
	public final static int PERMISSION_DENIED = 8;
	public final static int NO_CONSUMERS = 9;

	private int m_error = 0;
	private String m_message = "";

	/**
	 * @param error
	 * @param message
	 */
	public MessageServiceException(int error, String message) {

		m_error = error;
		m_message = message;
	}

	/**
	 * @return the error
	 */
	public int getError() {
		return m_error;
	}

	/**
	 * @param error
	 *            the error to set
	 */
	public void setError(int error) {
		m_error = error;
	}

	/**
	 * @return the message
	 */
	public String getMessage() {
		return m_message;
	}

	/**
	 * @param message
	 *            the message to set
	 */
	public void setMessage(String message) {
		m_message = message;
	}

	/**
	 * @return
	 */
	public boolean isBufferFull() {
		return (m_error == MessageServiceException.BUFFER_FULL);
	}

	/**
	 * @return
	 */
	public boolean isMessageServiceUninitialized() {
		return (m_error == MessageServiceException.SERVICE_UNINITIALIZED);
	}

	/**
	 * @return
	 */
	public boolean isTransportError() {
		return (m_error == MessageServiceException.TRANSPORT_ERROR);
	}
}
