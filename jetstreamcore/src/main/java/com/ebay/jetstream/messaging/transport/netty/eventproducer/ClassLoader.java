/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.eventproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClassLoader {
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");
	
	public static Object load(String className) {

	    Object obj = null;

	  
	    try {
	    	obj = Class.forName(className).newInstance();
	    }
	    catch (InstantiationException e) {

	      String message = "Exception while instatiating class - ";

	      message += e.getMessage();

	      LOGGER.error( message);

	      return null;
	    }
	    catch (IllegalAccessException e) {

	      LOGGER.error( "Exception while instatiating class - " + e.getLocalizedMessage());

	      return null;

	    }
	    catch (ClassNotFoundException e) {
	      String message = "Exception while instatiating class - ";

	      message += e.getMessage();

	      LOGGER.error( message);

	      return null;
	    }

	    return obj;
	  }

}
