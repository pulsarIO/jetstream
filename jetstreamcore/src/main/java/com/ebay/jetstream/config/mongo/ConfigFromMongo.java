/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.ErrorManager;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

public class ConfigFromMongo extends AbstractNamedBean implements XSerializable {
	
	private String m_mongoURL;
	private int m_noOfBeanUpdatesMax = 5;
	private Map<String,List<MongoConfigDetails>> m_configs = new HashMap<String,List<MongoConfigDetails>>();
	private Map<String,List<MongoConfigDetails>> m_failedConfigs = new HashMap<String,List<MongoConfigDetails>>();
	private ErrorManager m_errors = new ErrorManager();
	private static final String BEAN_NAME = "ConfigFromMongo";
	
	
	public ConfigFromMongo() {
		setBeanName(BEAN_NAME);
	}
	
	public String getMongoURL() {
		return m_mongoURL;
	}
	
	public ArrayList getSuccessfulConfig() {
		ArrayList displayConfigs = new ArrayList();
		Set<String> keys = m_configs.keySet();
		for(String key : keys) {
			List<MongoConfigDetails> mongoConfigs = m_configs.get(key);
			for(MongoConfigDetails mongoConfig : mongoConfigs) {
				displayConfigs.add(mongoConfig.getConfigDetails());
				//displayConfigs.add(mongoConfig);
			}
		}
		
		//return m_configs.toString();
		return displayConfigs;
	}
	
	public ArrayList getFailedConfig() {
		ArrayList displayConfigs = new ArrayList();
		Set<String> keys = m_failedConfigs.keySet();
		for(String key : keys) {
			List<MongoConfigDetails> mongoConfigs = m_failedConfigs.get(key);
			for(MongoConfigDetails mongoConfig : mongoConfigs) {
				displayConfigs.add(mongoConfig.getConfigDetails());
				//displayConfigs.add(mongoConfig);
			}
		}
		
		//return m_configs.toString();
		return displayConfigs;
	}
	
	
	public void setMongoURL(String mongoURL) {
		m_mongoURL = mongoURL;
	}
	
	public void setMongoConfigDetails(MongoConfigDetails config) {
		if(m_configs.containsKey(config.getBeanName())) {
			ArrayList configs = (ArrayList)m_configs.get(config.getBeanName());
			if(configs.size() >= m_noOfBeanUpdatesMax ) {
				configs.remove(0);
			}
			
			configs.add(config);
			m_configs.put(config.getBeanName(), configs);
			
		} else {
			List<MongoConfigDetails> configList = new ArrayList<MongoConfigDetails>();
			configList.add(config);
			m_configs.put(config.getBeanName(), configList);
		}
	}
	
	public void setFailedMongoConfigDetails(MongoConfigDetails config) {
		if(m_failedConfigs.containsKey(config.getBeanName())) {
			ArrayList configs = (ArrayList)m_failedConfigs.get(config.getBeanName());
			if(configs.size() >= m_noOfBeanUpdatesMax ) {
				configs.remove(0);
			}
			
			configs.add(config);
			m_failedConfigs.put(config.getBeanName(), configs);
			
		} else {
			List<MongoConfigDetails> configList = new ArrayList<MongoConfigDetails>();
			configList.add(config);
			m_failedConfigs.put(config.getBeanName(), configList);
		}
	}
	
	
}
