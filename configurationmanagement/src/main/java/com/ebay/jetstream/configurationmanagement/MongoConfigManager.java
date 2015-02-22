/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement;

import java.util.List;

import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;
import com.ebay.jetstream.config.mongo.MongoConfiguration;
import com.ebay.jetstream.config.mongo.MongoConnection;
import com.ebay.jetstream.config.mongo.MongoDAO;
import com.mongodb.BasicDBObject;

public class MongoConfigManager {
	private final MongoConnection mongoConnection;

	public MongoConfigManager(MongoConfiguration config) throws Exception {
		mongoConnection = new MongoConnection(config);
	}

	public int getMaxBeanVersion(String appName, String version,
			String beanName, String scope) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);
		query.put("scope", scope);

		List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO
				.findConfigurationByAppNameAndVersion(query, mongoConnection);
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

	public List<JetStreamBeanConfigurationDo> findAll() {
		List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO
				.findConfigurationByAppNameAndVersion(null, mongoConnection);

		return beanConfigs;
	}

	public List<JetStreamBeanConfigurationDo> getJetStreamConfiguration(
			String appName) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);

		List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO
				.findConfigurationByAppNameAndVersion(query, mongoConnection);

		return beanConfigs;
	}

	/**
	 * READ FROM DB
	 */

	public List<JetStreamBeanConfigurationDo> getJetStreamConfiguration(
			String appName, String version) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);

		List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO
				.findConfigurationByAppNameAndVersion(query, mongoConnection);

		return beanConfigs;
	}

	public List<JetStreamBeanConfigurationDo> getJetStreamConfiguration(
			String appName, String version, String beanName) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);

		List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO
				.findConfigurationByAppNameAndVersion(query, mongoConnection);

		return beanConfigs;
	}

	public List<JetStreamBeanConfigurationDo> getJetStreamConfiguration(
			String appName, String version, String beanName,
			String beanVersion, String scope) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);
		query.put("beanVersion", beanVersion);
		query.put("scope", scope);

		List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO
				.findConfigurationByAppNameAndVersion(query, mongoConnection);

		return beanConfigs;
	}

	public void uploadJetStreamConfiguration(
			JetStreamBeanConfigurationDo beanConfigDo) {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("appName", beanConfigDo.getAppName());
		dbObject.put("version", beanConfigDo.getVersion());
		dbObject.put("scope", beanConfigDo.getScope());

		dbObject.put("beanDefinition", beanConfigDo.getBeanDefinition());
		dbObject.put("beanVersion", beanConfigDo.getBeanVersion());
		dbObject.put("beanName", beanConfigDo.getBeanName());

		dbObject.put("createdBy", beanConfigDo.getCreatedBy());
		dbObject.put("modifiedBy", beanConfigDo.getModifiedBy());

		dbObject.put("creationDate", beanConfigDo.getCreationDate());
		dbObject.put("modifiedDate", beanConfigDo.getModifiedDate());

		MongoDAO.insertJetStreamConfiguration(dbObject, mongoConnection);
	}

	public boolean removeJetStreamConfiguration(String appName, String version,
			String beanName, String beanVersion, String scope) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);
		query.put("beanVersion", beanVersion);
		query.put("scope", scope);

		boolean result = MongoDAO.removeConfigurationByQuery(query,
				mongoConnection);

		return result;
	}

}