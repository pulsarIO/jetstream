/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.topic;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 */

/**
 * All Topics over which messages are being sent must be registered in this file.
 * 
 */

public class TopicDefs {

 
  // messaging topics

  public static final String JETSTREAM_EVENT_CONSUMER_DISCOVER_MSG = "Rtbdpod.Messaging/EventConsumerDiscover";

  public static final String JETSTREAM_EVENT_CONSUMER_ADVISORY_MSG = "Rtbdpod.Messaging/EventConsumerAdvisory";

  public static final String JETSTREAM_EVENT_CONSUMER_ADVERTISEMENT_MSG = "Rtbdpod.Messaging/EventConsumerAdvertisement";

  public static final String JETSTREAM_MESSAGING_INTERNAL_STATE_ADVISORY = "Rtbdpod.Messaging/InternalStateAdvisory";
  // Affinity Topics
  public static final String JETSTREAM_AFFINITY_CONFIG_DISCOVER_MSG = "Rtbdpod.Messaging/DiscoverConfig";

  public static final String JETSTREAM_AFFINITY_CONFIG_REPLY_MSG = "Rtbdpod.Messaging/ConfigReply";

  public static final String JETSTREAM_WEIGHT_CONFIG_CHANGE_MSG = "Rtbdpod.Messaging/WeightConfigChange";

  public static final String JETSTREAM_AFFINITY_PING_CONFIG_MSG = "Rtbdpod.Messaging/PingConfig";

  public static final String JETSTREAM_AFFINITY_CONFIG_PING_FAILURE_MSG = "Rtbdpod.Messaging/PingFailure";

  public static final String JETSTREAM_AFFINITY_POOL_RESIZE_MSG = "Rtbdpod.Messaging/PoolResize";

  public static final String JETSTREAM_AFFINITY_ACTIVITY_CHANGE_MSG = "Rtbdpod.Messaging/ft.group/affinityConfiguratorActivityChange";

  public static final String JETSTREAM_AFFINITY_SYNC_CONFIG_MSG = "Rtbdpod.AffinityConfigurator/SyncConfig";

  public static final String JETSTREAM_AFFINITY_SLAVE_ADVERTISE_MSG = "Rtbdpod.AffinityConfigurator/SlaveAdvertisement";

   // LDAP Config Change Notification Event
  public static final String JETSTREAM_CONFIGCHANGE_EVENT = "Rtbdpod.local/configChange";
  
  public static final String JETSTREAM_FTGROUP_MEMBERSHIP_ADVERTISEMENT = "Rtbdpod.Messaging/GMA";

}
