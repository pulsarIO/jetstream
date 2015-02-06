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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;
import com.ebay.jetstream.config.mongo.MongoConfiguration;
import com.ebay.jetstream.configurationmanagement.model.JetStreamBeanConfigurationLogDo;
import com.mongodb.BasicDBObject;

public class MongoConfigLogManager {
	private MongoLogConnection mongoLogConnection;

	public MongoConfigLogManager(MongoConfiguration config) throws Exception {
		mongoLogConnection = new MongoLogConnection(config);
	}

	public List<JetStreamBeanConfigurationLogDo> findAll() {
		List<JetStreamBeanConfigurationLogDo> beanConfigs = MongoLogDAO
				.findConfigurationByAppNameAndVersion(null, mongoLogConnection);

		return beanConfigs;
	}

	public List<JetStreamBeanConfigurationLogDo> getJetStreamConfiguration(
			String appName) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);

		List<JetStreamBeanConfigurationLogDo> beanConfigs = MongoLogDAO
				.findConfigurationByAppNameAndVersion(query, mongoLogConnection);

		return beanConfigs;
	}

	/**
	 * READ FROM DB
	 */

	public List<JetStreamBeanConfigurationLogDo> getJetStreamConfiguration(
			String appName, String version) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);

		List<JetStreamBeanConfigurationLogDo> beanConfigs = MongoLogDAO
				.findConfigurationByAppNameAndVersion(query, mongoLogConnection);

		return beanConfigs;
	}

	public List<JetStreamBeanConfigurationLogDo> getJetStreamConfiguration(
			String appName, String version, String beanName) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);

		List<JetStreamBeanConfigurationLogDo> beanConfigs = MongoLogDAO
				.findConfigurationByAppNameAndVersion(query, mongoLogConnection);

		return beanConfigs;
	}

	public List<JetStreamBeanConfigurationLogDo> getJetStreamConfiguration(
			String appName, String version, String beanName, String scope) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);
		query.put("scope", scope);

		List<JetStreamBeanConfigurationLogDo> beanConfigs = MongoLogDAO
				.findConfigurationByAppNameAndVersion(query, mongoLogConnection);
		return beanConfigs;

	}

	public List<JetStreamBeanConfigurationLogDo> getJetStreamConfiguration(
			String appName, String version, String beanName,
			String beanVersion, String scope) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);
		query.put("beanVersion", beanVersion);
		query.put("scope", scope);

		List<JetStreamBeanConfigurationLogDo> beanConfigs = MongoLogDAO
				.findConfigurationByAppNameAndVersion(query, mongoLogConnection);

		return beanConfigs;
	}

	public void uploadJetStreamConfiguration(
			JetStreamBeanConfigurationLogDo beanConfigLogDo) {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("appName", beanConfigLogDo.getAppName());
		dbObject.put("version", beanConfigLogDo.getVersion());
		dbObject.put("scope", beanConfigLogDo.getScope());

		dbObject.put("beanDefinition", beanConfigLogDo.getBeanDefinition());
		dbObject.put("beanVersion", beanConfigLogDo.getBeanVersion());
		dbObject.put("beanName", beanConfigLogDo.getBeanName());

		dbObject.put("createdBy", beanConfigLogDo.getCreatedBy());
		dbObject.put("modifiedBy", beanConfigLogDo.getModifiedBy());
		dbObject.put("operatedBy", beanConfigLogDo.getOperatedBy());

		dbObject.put("creationDate", beanConfigLogDo.getCreationDate());
		dbObject.put("modifiedDate", beanConfigLogDo.getModifiedDate());
		dbObject.put("operatedDate", beanConfigLogDo.getOperatedDate());

		dbObject.put("status", beanConfigLogDo.getStatus());

		MongoLogDAO.insertJetStreamConfiguration(dbObject, mongoLogConnection);
	}

	public int getMaxBeanVersion(String appName, String version,
			String beanName, String scope) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);
		query.put("scope", scope);

		List<JetStreamBeanConfigurationLogDo> beanConfigs = MongoLogDAO
				.findConfigurationByAppNameAndVersion(query, mongoLogConnection);
		int maxVersion = -1;
		for (int i = 0; i < beanConfigs.size(); i++) {
			JetStreamBeanConfigurationDo config = beanConfigs.get(i);
			String beanVersion = config.getBeanVersion();
			int beanVersionInt = Integer.parseInt(beanVersion);
			if (i == 0 || beanVersionInt > maxVersion) {
				maxVersion = Integer.parseInt(beanVersion);
			}
		}
		return maxVersion;
	}

	public void removeBeanLogBefore(String dateStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = sdf.parse(dateStr);
		BasicDBObject condition = new BasicDBObject();
		condition.put("$lt", date.getTime());
		BasicDBObject query = new BasicDBObject();
		query.put("operatedDate", condition);
		MongoLogDAO.removeConfigurationByQuery(query, mongoLogConnection);
	}
}