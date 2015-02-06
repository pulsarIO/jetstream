/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.interfaces;

import java.util.List;

import com.ebay.jetstream.messaging.DispatchQueueStats;
import com.ebay.jetstream.messaging.exception.MessageServiceException;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

/**
 * 
 * 
 * @author shmurthy (shmurthy@ebay.com)
 * 
 *  Interface implemented by MessageService. This interface is used by
 *  Transport implementations to send received messages and advisory messages 
 *  upstream. Advisory messages are messages that indicate the state of the
 *  transport or to report the failure of sending messages downstream 
 */

public interface ITransportListener {

	/**
	 * @param tm
	 * @param stats
	 * @throws MessageServiceException
	 */
	void postMessage(JetstreamMessage tm, DispatchQueueStats stats) throws MessageServiceException;

    /**
     * @param tm
     */
    void postAdvise(JetstreamMessage tm);

    /**
     * @param msgs
     * @param m_queueStats
     * @throws MessageServiceException
     */
    void postMessage(List<JetstreamMessage> msgs, DispatchQueueStats m_queueStats)  throws MessageServiceException;
}
