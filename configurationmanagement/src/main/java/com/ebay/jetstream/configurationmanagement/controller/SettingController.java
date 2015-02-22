/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.controller;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;
import com.ebay.jetstream.configurationmanagement.JsonUtil;
import com.ebay.jetstream.configurationmanagement.model.AppConfiguration;
import com.ebay.jetstream.configurationmanagement.model.ResponseResult;
import com.ebay.jetstream.mongo.tools.PublishConfigMessage;
import com.google.gson.reflect.TypeToken;

@Controller
@RequestMapping(value = "/settings")
public class SettingController extends AbstractController {
	@Autowired
	private PublishConfigMessage configMessagePublisher;

	@RequestMapping(method = RequestMethod.GET)
	public String entry(Model model) {
		return "settings";
	}

	private Set<AppConfiguration> getAllApp() throws Exception {
		List<JetStreamBeanConfigurationDo> jetStreamConfigs = mongoConfigManager
				.getJetStreamConfiguration(SETTINGS, "1.0", APP_CONFIG);
		if (jetStreamConfigs.isEmpty()) {
			Set<AppConfiguration> set = new TreeSet<AppConfiguration>();
			return set;
		}

		JetStreamBeanConfigurationDo jetStreamConfig = jetStreamConfigs.get(0);
		Type type = new TypeToken<TreeSet<AppConfiguration>>() {}.getType();
		Set<AppConfiguration> set = JsonUtil.fromJson(jetStreamConfig.getBeanDefinition(), type);
		return set;
	}

	private void updateApp(Set<AppConfiguration> set) {
		JetStreamBeanConfigurationDo doObj = new JetStreamBeanConfigurationDo();
		doObj.setAppName(SETTINGS);
		doObj.setBeanName(APP_CONFIG);
		doObj.setVersion("1.0");
		doObj.setBeanVersion("1");
		doObj.setScope("global");
		doObj.setBeanDefinition(JsonUtil.toJson(set));
		mongoConfigManager.uploadJetStreamConfiguration(doObj);
	}

	@RequestMapping(value = "/listAllApp", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
	public @ResponseBody
	ResponseResult findAllApp() throws Exception {
		try {
			return new ResponseResult(true, "", getAllApp());
		} catch (Exception ex) {
			return new ResponseResult(false, "listAll failed due to: " + ex.getMessage());
		}
	}

	@RequestMapping(value = "/createApp", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult createApp(@RequestBody AppConfiguration request) throws Exception {
		try {
			Set<AppConfiguration> set = getAllApp();
			if (set.contains(request)) {
				return new ResponseResult(false, "duplicate app name: " + request.getName());
			}

			set.add(request);

			if (!mongoConfigManager.removeJetStreamConfiguration(SETTINGS, "1.0", APP_CONFIG, "1", "global")) {
				return new ResponseResult(false, "createApp failed.");
			}

			updateApp(set);

			return new ResponseResult(true);
		} catch (Exception ex) {
			return new ResponseResult(false, "createApp failed due to: " + ex.getMessage());
		}
	}

	@RequestMapping(value = "/updateApp", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult updateApp(@RequestBody AppConfiguration request) throws Exception {
		try {
			Set<AppConfiguration> set = getAllApp();
			set.remove(request);
			set.add(request);

			if (!mongoConfigManager.removeJetStreamConfiguration(SETTINGS, "1.0", APP_CONFIG, "1", "global")) {
				return new ResponseResult(false, "updateApp failed.");
			}

			updateApp(set);
			
			return new ResponseResult(true);
		} catch (Exception ex) {
			return new ResponseResult(false, "updateApp failed due to: " + ex.getMessage());
		}
	}

	@RequestMapping(value = "/deleteApp", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult delete(@RequestBody AppConfiguration request) {
		try {
			Set<AppConfiguration> set = getAllApp();
			set.remove(request);
			
			if (!mongoConfigManager.removeJetStreamConfiguration(SETTINGS, "1.0", APP_CONFIG, "1", "global")) {
				return new ResponseResult(false, "deleteApp failed.");
			}

			updateApp(set);
			
			return new ResponseResult(true);

		} catch (Exception ex) {
			return new ResponseResult(false, "deleteApp failed due to: "
					+ ex.getMessage());
		}
	}

	@RequestMapping(value = "/updateDC", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult updateDC(HttpServletRequest request,
			HttpServletResponse response) {
		try {
			boolean result = mongoConfigManager.removeJetStreamConfiguration(
					SETTINGS, "1.0", DATA_CENTERS, "1", "global");
			if (!result) {
				return new ResponseResult(false, "Failed to remove DC.");
			}

			String dcList = parse2String(request.getParameter("list"));
			String beanDefinition = "dcList:" + dcList;
			JetStreamBeanConfigurationDo doObj = new JetStreamBeanConfigurationDo();
			doObj.setAppName(SETTINGS);
			doObj.setBeanName(DATA_CENTERS);
			doObj.setVersion("1.0");
			doObj.setBeanVersion("1");
			doObj.setScope("global");
			doObj.setBeanDefinition(beanDefinition);
			mongoConfigManager.uploadJetStreamConfiguration(doObj);
			
			return new ResponseResult(true, "", dcList);
		} catch (Exception ex) {
			return new ResponseResult(false, "updateDC failed due to: "
					+ ex.getMessage());
		}
	}

	private String parse2String(String source){
		String[] list = source.split("\\r\\n{1,}");
		if(list.length <= 1)
			list = source.split("\\n{1,}");
		list = removeDuplicate(list);
		StringBuilder buffer = new StringBuilder();
		for (String app : list) {
			buffer.append(app).append(",");
		}
		return buffer.substring(0, buffer.length() -1);
	}

	private String[] removeDuplicate(String[] source) {
		if (source == null)
			return null;
		TreeSet<String> treeSet = new TreeSet<String>();
		for (String str : source) {
			treeSet.add(str);
		}

		return treeSet.toArray(new String[0]);
	}
}
