/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty;

import java.util.concurrent.atomic.AtomicInteger;

import com.ebay.jetstream.messaging.interfaces.IMessageListener;
import com.ebay.jetstream.messaging.messagetype.BytesMessage;
import com.ebay.jetstream.messaging.messagetype.JetstreamMessage;

public class MsgListener implements IMessageListener {
	private AtomicInteger m_count = new AtomicInteger(0);

	public int getCount() {
		return m_count.get();
	}

	public void setCount(int count) {
		m_count.set(count);
	}
	
	public MsgListener() {
	}

	public MsgListener(BytesMessage bm) {
	}

	@Override
	public void onMessage(JetstreamMessage m) {

		if (m instanceof BytesMessage) {

			m_count.addAndGet(1);
		}
	}

}
