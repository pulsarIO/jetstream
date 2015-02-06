/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor;

import com.ebay.jetstream.event.JetstreamEvent;

/**
 * This is very special kind of event serving one and only one purpose: to let processor thread know that it's time to
 * say goodbye.
 *
 * @author snikolaev
 *
 */
public class ShutdownJetstreamEvent extends JetstreamEvent {

}
