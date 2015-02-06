/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel;

import com.ebay.jetstream.config.AbstractNamedBean;

/**
 * A ChannelAddress represents a way of identifying a channel in a global name space. The base class is simply used to
 * tag the various types of channel addresses that can exist.
 * 
 * It extends AbstractNamedBean so that Spring-integrated bean change detection can be done by bean name comparison.
 * 
 * @author Dan Pritchett, Mark Sikes
 * 
 * @see com.ebay.jetstream.config.AbstractNamedBean
 */
public abstract class ChannelAddress extends AbstractNamedBean {

}
