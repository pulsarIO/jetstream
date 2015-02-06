/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.application;

/**
 * @author shmurthy
 * 
 *        A work request that can be executed  by the the container's workers
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WorkRequest implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.application");

	@Override
	public void run() {

		try {
			execute();
		} catch (Throwable t) {
			LOGGER.error(
					"failed to execute task : " + t.getLocalizedMessage());
		}
	}

	public abstract void execute();

}
