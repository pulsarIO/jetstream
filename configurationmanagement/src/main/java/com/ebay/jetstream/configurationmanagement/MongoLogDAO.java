/*******************************************************************************
 * Copyright 2012-2015 eBay Software Foundation
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;

import com.ebay.jetstream.config.mongo.MongoConfigRuntimeException;
import com.ebay.jetstream.configurationmanagement.model.JetStreamBeanConfigurationLogDo;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class MongoLogDAO {

	public static List<JetStreamBeanConfigurationLogDo> findConfigurationByAppNameAndVersion(
			BasicDBObject query, MongoLogConnection mongoConnection) {

		List<JetStreamBeanConfigurationLogDo> beanConfigs = new ArrayList<JetStreamBeanConfigurationLogDo>();
		List<BasicDBObject> dbObjects = new ArrayList<BasicDBObject>();
		DBCollection dbCol = mongoConnection.getDBCollection();

		if (dbCol == null) {
			throw new MongoConfigRuntimeException(
					"jetstreamconfiglog collection is unknown");
		}

		Exception e = null;
		DBCursor cur = null;
		try {
			cur = (query == null ? dbCol.find() : dbCol.find(query));
			while (cur.hasNext()) {
				dbObjects.add((BasicDBObject) cur.next());
			}

			for (BasicDBObject dbObject : dbObjects) {
				String jsonString = dbObject.toString();
				beanConfigs.add(unMarshalJSONResponse(jsonString));
			}
		} catch (Exception err) {
			e = err;
			throw new MongoConfigRuntimeException(err);
		} finally {
			if (cur != null) {
				cur.close();
			}
		}

		return beanConfigs;
	}

	public static List<JetStreamBeanConfigurationLogDo> findConfigurationByQuery(
			BasicDBObject query, MongoLogConnection mongoLogConnection) {

		List<JetStreamBeanConfigurationLogDo> beanConfigs = new ArrayList<JetStreamBeanConfigurationLogDo>();
		List<BasicDBObject> dbObjects = new ArrayList<BasicDBObject>();
		DBCollection dbCol = mongoLogConnection.getDBCollection();

		if (dbCol == null) {
			throw new MongoConfigRuntimeException(
					"jetstreamconfigLog collection is unknown");
		}

		Exception e = null;

		DBCursor cur = null;
		try {
			cur = (query == null ? dbCol.find() : dbCol.find(query));
			while (cur.hasNext()) {
				dbObjects.add((BasicDBObject) cur.next());
			}

			for (BasicDBObject dbObject : dbObjects) {
				String jsonString = dbObject.toString();
				beanConfigs.add(unMarshalJSONResponse(jsonString));
				// beanConfig =
				// (JetStreamBeanConfigurationDo)fromJson(jsonString,
				// JetStreamBeanConfigurationDo.class);
			}
		} catch (Exception err) {
			e = err;
			throw new MongoConfigRuntimeException(err);
		} finally {
			if (cur != null) {
				cur.close();
			}
		}

		return beanConfigs;
	}

	private static JetStreamBeanConfigurationLogDo unMarshalJSONResponse(
			String jsonString) {
		try {
			JetStreamBeanConfigurationLogDo result = null;
			if (jsonString != null) {
				try {
					ObjectMapper mapper = new ObjectMapper();
					mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
					result = mapper.readValue(jsonString,
							JetStreamBeanConfigurationLogDo.class);
				} catch (Exception e) {
					throw new MongoConfigRuntimeException(e);
				}
			}

			return result;

		} catch (Throwable e) {
			throw new MongoConfigRuntimeException(e);
		}
	}

	/**
	 * UPLOAD TO DB
	 */
	public static void insertJetStreamConfiguration(BasicDBObject dbObject,
			MongoLogConnection mongoLogConnection) {
		JetStreamBeanConfigurationLogDo beanConfig = null;
		DBCollection dbCol = mongoLogConnection.getDBCollection();

		if (dbCol == null) {
			throw new MongoConfigRuntimeException(
					"jetstreamconfig collection is unknown");
		}

		WriteResult result = dbCol.insert(dbObject);
		if (result.getError() != null) {
			throw new MongoConfigRuntimeException(result.getError());
		}
	}

	public static boolean removeConfigurationByQuery(BasicDBObject query,
			MongoLogConnection mongoLogConnection) {

		DBCollection dbCol = mongoLogConnection.getDBCollection();

		if (dbCol == null) {
			throw new MongoConfigRuntimeException(
					"jetstreamconfig collection is unknown");
		}

		try {
			if (query == null) {
				return false;
			}

			WriteResult result = dbCol.remove(query, WriteConcern.SAFE);

			if (result.getLastError().ok()) {
				return true;
			}

		} catch (Exception err) {
			throw new MongoConfigRuntimeException(err);
		}

		return true;
	}
}
