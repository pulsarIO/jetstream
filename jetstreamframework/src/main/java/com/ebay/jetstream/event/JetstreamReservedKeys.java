/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event;

import java.util.HashSet;
import java.util.Set;

/**
 * The JetstreamReservedKeys enum defines the event keys that are reserved and interpreted by the Jetstream eventlet
 * framework. Adapted from Jetstream
 * 
 * @author shmurthy@ebay.com
 * 
 */
public enum JetstreamReservedKeys {
	/**
	 * The event id key defines the reserved key used to represent event id for access within the map. This key represents
	 * a read-only value in the map.
	 */
	EventId("js_ev_id"),

	/**
	 * The event type defines the category of this an event.
	 */
	EventType("js_ev_type"),

	/**
	 * the rate at which the event should be processed by EventProcessor.
	 */
	EventRate("js_ev_rate"),
	/**
	 * The event type defines the category of this an event.
	 */
	WorkerId("js_ev_wid"),

	/**
	 * The event time defines the time the event arrived at Router
	 */
	EventTime("js_ev_time"),

	/**
	 * The event time defines the time the event arrived at Cerulean (RES Sync layer).
	 */
	CallingPlatform("js_ev_src"),

	/**
	 * Holds the Message affinity key - This should be present in the outermost Map in the JetstreamEvent Structure.
	 * Intended to be used to Signal the messaging stack to use this value to compute the split to which the associated
	 * message is to be forwarded to.
	 */

	MessageAffinityKey("js_ev_mak"),

	/**
	 * Holds the respective guid value for a given scope (source, destination or link)
	 */
	GuidKey("js_guid"),

	/**
	 * Holds the cause for which the event was tagged invalid
	 */
	Invalid("INVALID_EVENT"),

	EventOrigin("js_ev_orgn"),

	JetstreamEventHolder("js_ev_evholder"),

	EventSource("js_ev_src"),

	EventBatch("js_ev_batch"),


	/**
	 * Used in SFS for storing the events
	 */
	EventIndex("js_ev_ndx"),

	RetryCount("js_erc"),
	/**
	 * Used in SFS to persist the timestamp at which the event was written
	 */
	EventWriteTime("js_ev_ev_write_time"),
	
	EventMetaData("js_ev_metadata"),
	
	EventReplayTopic("js_rplytpc"),
	
	EventReplay("js_rply"), 
	
	EventBroadCast("js_bc"),
	
	AbusiveEntity("js_ae"),
	
	EventKafkaMeta("js_km");

	private final String m_key;
	private volatile static Set<String> RESERVED_STRINGS;

	private JetstreamReservedKeys(String key) {
		m_key = key;
		store(key);
	}

	@Override
	public String toString() {
		return m_key;
	}

	public static boolean isReserved(String strKey) {
		return RESERVED_STRINGS.contains(strKey.toLowerCase());
	}

	private void store(String strKey) {
		if (RESERVED_STRINGS == null) {
			synchronized(JetstreamReservedKeys.class) {
				if (RESERVED_STRINGS == null)
					RESERVED_STRINGS = new HashSet<String>();
			}
		}
		RESERVED_STRINGS.add(strKey.toLowerCase());
	}

}
