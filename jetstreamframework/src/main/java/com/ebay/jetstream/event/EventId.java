/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import java.io.Serializable;

import com.ebay.jetstream.xmlser.XSerializable;


/**
 * EventId is an immutable identifier for an event. The property of the EventId
 * is that no two events will share this identifier. It will be used throughout the event
 * system to manage the processing state of an event.
 * 
 * Implementations of EventId must implement the standard equals and hashCode methods
 * correctly so that the object may be used in java.util.Map objects.
 * 
 * @see java.util.Map
 * 
 * @author Dan Pritchett
 *
 */
public interface EventId extends XSerializable, Serializable {
	/**
	 * The getOpaqueHandle method must return a string version of the identifier. Implementations
	 * may chose to serialize the handle to string in any fashion. The serialization must be stable
	 * and must not change the immutable and uniqueness properties of the identifer.
	 * 
	 * @return the serialized form.
	 */
	public String getOpaqueHandle();
}
