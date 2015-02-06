/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.messagetype;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * @author shmurthy
 *
 * A class that is used to send a message containingg a byte array. This calls accepts a
 * byte message and marshalls the same
 *  
 */

public class BytesMessage extends JetstreamMessage implements Externalizable {

	/**
   * 
   */
	private static final long serialVersionUID = 1L;

	private byte[] m_byteMessage = new byte[2];

	public BytesMessage() {
	}

	public void readExternal(ObjectInput in) throws IOException {

		super.readExternal(in);

		int size = in.readInt();

		m_byteMessage = new byte[size];

		in.readFully(m_byteMessage);

	}

	public void writeExternal(ObjectOutput out) throws IOException {

		super.writeExternal(out);

		out.writeInt(m_byteMessage.length);

		out.write(m_byteMessage);

	}

	/**
	 * @return the byteMessage
	 */
	public byte[] getByteMessage() {
		return m_byteMessage;
	}

	/**
	 * @param byteMessage
	 *            the byteMessage to set
	 */
	public void setByteMessage(byte[] byteMessage) {
		m_byteMessage = byteMessage;
	}

}
