/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.messagetype;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


/**
 * 
 * 
 * @author shmurthy (shmurthy@ebay.com)
 * This is a wrapper for sending any java serializable object in a message.
 */

public class Any extends JetstreamMessage implements KryoSerializable {

	private static final long serialVersionUID = 1L;

	private Object m_obj = new Object();

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	/**
	 * 
	 */
	public Any() {
	}

	/**
	 * @param obj
	 */
	public Any(Object obj) {
		m_obj = obj;
	}

	/**
	 * @param obj
	 */
	public void setObject(Object obj) {
		m_obj = obj;
	}

	/**
	 * @return
	 */
	public Object getObject() {
		return m_obj;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessage#writeExternal(java.io.ObjectOutput
	 * )
	 */
	public void writeExternal(ObjectOutput out) throws IOException {

		super.writeExternal(out);

		out.writeObject(m_obj);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.messaging.JetstreamMessage#readExternal(java.io.ObjectInput
	 * )
	 */
	public void readExternal(ObjectInput in) throws IOException {

		try {
			super.readExternal(in);

			m_obj = in.readObject();

		} catch (Exception e) {

			if (LOGGER.isWarnEnabled()) {
				String message = "Unmarshalling Error  - ";
				message += e.getMessage();

				LOGGER.warn( message, e);

			}
			

		}

	}

	public String toString() {

		return m_obj.toString();
	}

    @Override
    public void write(Kryo kryo, Output output) {
        super.writeKryo(kryo, output);

        kryo.writeClassAndObject(output, m_obj);
        
    }

    @Override
    public void read(Kryo kryo, Input input) {
        super.readKryo(kryo, input);
        
        m_obj = kryo.readClassAndObject(input);
        
    }

}
