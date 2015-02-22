/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement;

import com.ebay.jetstream.config.mongo.MongoConfiguration;
import com.ebay.jetstream.config.mongo.MongoConnection;
import com.mongodb.DBCollection;

public class MongoLogConnection extends MongoConnection {
	private static final String collection = "jetstreamconfiglog";

	public MongoLogConnection(MongoConfiguration mongoConfiguration) {
		super(mongoConfiguration);
	}

	public DBCollection getDBCollection() {
		return (getDB() == null ? null : getDB().getCollection(collection));
	}

}
