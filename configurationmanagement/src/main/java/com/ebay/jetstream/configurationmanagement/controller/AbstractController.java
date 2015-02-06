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
package com.ebay.jetstream.configurationmanagement.controller;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;
import com.ebay.jetstream.configurationmanagement.JsonUtil;
import com.ebay.jetstream.configurationmanagement.MongoConfigLogManager;
import com.ebay.jetstream.configurationmanagement.MongoConfigManager;
import com.ebay.jetstream.configurationmanagement.model.AppConfiguration;
import com.google.gson.reflect.TypeToken;

@Controller
public abstract class AbstractController {
	
	@Autowired
	protected MongoConfigManager mongoConfigManager;
	
	@Autowired
	protected MongoConfigLogManager mongoConfigLogManager;

	protected static final String SETTINGS = "JETSTREAM_CONFIGURATION_SETTINGS";
	protected static final String APP_CONFIG = "APP_CONFIG";
	protected static final String DATA_CENTERS = "DATA_CENTERS";
	protected static final String APP_NAME_LIST = "appNameList";
	protected static final String DATA_CENTER_LIST = "dcList";
	protected static final String DATA_FLOW = "dataFlow";
	protected static final String ERROR = "error";
	protected static final String TRACKING_BEAN_NAME = "userTrackingService";


	/**
	 * TODO We should return the a setting object.
	 * @return
	 */
	protected Map<String, String[]> getProperties() {
		Map<String, String[]> result = new HashMap<String, String[]>();
		List<JetStreamBeanConfigurationDo> appObjList = mongoConfigManager.getJetStreamConfiguration(SETTINGS, "1.0", APP_CONFIG);
		List<JetStreamBeanConfigurationDo> dcObjList = mongoConfigManager.getJetStreamConfiguration(SETTINGS, "1.0", DATA_CENTERS);
		if (appObjList.isEmpty()) {
			result.put(APP_NAME_LIST, new String[] {APP_CONFIG, DATA_CENTERS});
		} else {
			String[] appList = getAppNames(appObjList.get(0));
			if(appList != null) {
				result.put(APP_NAME_LIST, appList);
			}
		}

		if(dcObjList.isEmpty()){
			result.put(DATA_CENTER_LIST, new String[] {DATA_CENTERS});
		}else{
			result.put(DATA_CENTER_LIST, parse(dcObjList.get(0)));
		}

		return result;
	}
	

	private String[] getAppNames(JetStreamBeanConfigurationDo doObj) {
		String beanDefinition = doObj.getBeanDefinition();
		Type type = new TypeToken<TreeSet<AppConfiguration>>() {}.getType();
		Set<AppConfiguration> set = JsonUtil.fromJson(beanDefinition, type);
		String[] appNames = new String[set.size()];
		int i = 0;
		for (AppConfiguration appConfig : set) {
			appNames[i++] = appConfig.getName();
		}

		return appNames;
	}
	/**
	 * This is used for parse App list and DC list.
	 * 
	 * @param obj
	 * @return
	 */
	private String[] parse(JetStreamBeanConfigurationDo obj){
		if(obj == null)
			return null;
		String beanDefinition = obj.getBeanDefinition();
		/**
		 * TODO appList:a1,a2,a3; as a initial version: it's just simple string
		 * splited by";", but for long run it's should be the XML format or json
		 * format.
		 */
		try {
			String[] results = beanDefinition.split(":")[1].split(",");
			return results;
		} catch (RuntimeException ex) {
			return null;
		}
	}
	
	public AppConfiguration getAppConfigByApp(String name){
		if(name == null) {
			return null;
		}
		List<JetStreamBeanConfigurationDo> jetStreamConfigs = mongoConfigManager.getJetStreamConfiguration(SETTINGS, "1.0", APP_CONFIG);
		if(!jetStreamConfigs.isEmpty()){
			String beanDefinition = jetStreamConfigs.get(0).getBeanDefinition();
			Type type = new TypeToken<TreeSet<AppConfiguration>>() {}.getType();
			Set<AppConfiguration> set = JsonUtil.fromJson(beanDefinition, type);
			for(AppConfiguration appConfig : set){
				if(appConfig.getName().equals(name))
					return appConfig;
			}
		}
		return null;
	}
	
	public String getTrackingAppIP(){
		List<JetStreamBeanConfigurationDo> jetStreamConfigs = mongoConfigManager.getJetStreamConfiguration(SETTINGS, "1.0", TRACKING_BEAN_NAME);
		if(jetStreamConfigs.isEmpty())
			return null;
		else
			return jetStreamConfigs.get(0).getBeanDefinition();
	}

}
