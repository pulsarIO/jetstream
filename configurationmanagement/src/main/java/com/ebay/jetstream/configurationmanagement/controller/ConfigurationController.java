/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;
import com.ebay.jetstream.configurationmanagement.ConfigurationManagementXMLParser;
import com.ebay.jetstream.configurationmanagement.DataValidators;
import com.ebay.jetstream.configurationmanagement.model.JetStreamBeanConfiguration;
import com.ebay.jetstream.configurationmanagement.model.JetStreamBeanConfigurationLog;
import com.ebay.jetstream.configurationmanagement.model.JetStreamBeanConfigurationLogDo;
import com.ebay.jetstream.configurationmanagement.model.ResponseResult;
import com.ebay.jetstream.mongo.tools.PublishConfigMessage;

/**
 * The MVC controller class for Mongo Configuration Management
 * 
 * @author weijin
 * 
 */
@Controller
@RequestMapping(value = "/configuration")
public class ConfigurationController extends AbstractController {
	private static final int UPDATED_STATUS = 0;
	private static final int DELETED_STATUS = 1;

	@Autowired
	private PublishConfigMessage configMessagePublisher;

	@Autowired
	private ConfigurationManagementXMLParser cmgXMLParser;

	private static final String BEGIN = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			+ "\n"
			+ "<beans xmlns=\"http://www.springframework.org/schema/beans\""
			+ "\n"
			+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
			+ "\n"
			+ "xsi:schemaLocation=\"http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-4.0.xsd\""
			+ "\n" + "default-lazy-init=\"false\">" + "\n\n";

	private static final String END = "\n\n</beans>";

	private static final Comparator<JetStreamBeanConfigurationDo> JETSTREAM_CONFIG_COMPARATOR = new Comparator<JetStreamBeanConfigurationDo>() {
		public int compare(JetStreamBeanConfigurationDo doObj1,
				JetStreamBeanConfigurationDo doObj2) {
			if (doObj1.getModifiedDate() > doObj2.getModifiedDate()) {
				return -1;
			} else if (doObj1.getModifiedDate() == doObj2.getModifiedDate()) {
				return 0;
			} else {
				return 1;
			}
		}
	};

	@RequestMapping
	public String entry(HttpServletRequest httpRequest,
			HttpServletResponse httpResponse) {
		Map<String, String[]> properties = this.getProperties();
		httpRequest.setAttribute(APP_NAME_LIST, properties.get(APP_NAME_LIST));
		httpRequest.setAttribute(DATA_CENTER_LIST,
				properties.get(DATA_CENTER_LIST));
		return "home";
	}

	@RequestMapping(value = "/listAll", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
	public @ResponseBody
	List<JetStreamBeanConfiguration> findAll() throws Exception {
		List<JetStreamBeanConfigurationDo> doList = mongoConfigManager
				.findAll();
		Collections.sort(doList, JETSTREAM_CONFIG_COMPARATOR);
		List<JetStreamBeanConfiguration> toList = new ArrayList<JetStreamBeanConfiguration>(
				doList.size());
		for (JetStreamBeanConfigurationDo doObj : doList) {
			// if(!doObj.getAppName().equals(SETTINGS))
			toList.add(JetStreamBeanConfiguration.convert(doObj));
		}

		return toList;
	}

	@RequestMapping(value = "/listByApp", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.POST)
	public @ResponseBody
	List<JetStreamBeanConfiguration> findByApp(
			@RequestBody JetStreamBeanConfigurationDo request) throws Exception {
		List<JetStreamBeanConfigurationDo> doList = mongoConfigManager
				.getJetStreamConfiguration(request.getAppName());
		Collections.sort(doList, JETSTREAM_CONFIG_COMPARATOR);

		List<JetStreamBeanConfiguration> toList = new ArrayList<JetStreamBeanConfiguration>(
				doList.size());
		for (JetStreamBeanConfigurationDo doObj : doList) {
			toList.add(JetStreamBeanConfiguration.convert(doObj));
		}

		return toList;
	}

	@RequestMapping(value = "/listBeanLog", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
	public @ResponseBody
	List<JetStreamBeanConfigurationLog> getBeanLog() {
		List<JetStreamBeanConfigurationLogDo> doList = mongoConfigLogManager
				.findAll();
		Collections.sort(doList, JETSTREAM_CONFIG_COMPARATOR);
		List<JetStreamBeanConfigurationLog> toList = new ArrayList<JetStreamBeanConfigurationLog>(
				doList.size());
		for (JetStreamBeanConfigurationLogDo doObj : doList) {
			toList.add(JetStreamBeanConfigurationLog.convert(doObj));
		}
		return toList;
	}

	private String getBeanDefinition(String beanDefinition)
			throws UnsupportedEncodingException {
		return BEGIN + URLDecoder.decode(beanDefinition, "UTF-8") + END;
	}

	protected void checkDuplicateBean(JetStreamBeanConfigurationDo request) {
		List<JetStreamBeanConfigurationDo> list = mongoConfigManager
				.getJetStreamConfiguration(request.getAppName(),
						request.getVersion(), request.getBeanName(),
						request.getBeanVersion(), request.getScope());
		if (list != null && list.size() > 0) {
			throw new RuntimeException("Duplicate bean detected. Bean id = "
					+ request.getBeanName());
		}
	}

	private void validateRequest(JetStreamBeanConfigurationDo request)
			throws Exception {
		DataValidators.NOT_NULL_OR_EMPTY.validate("app name",
				request.getAppName());
		DataValidators.NOT_NULL_OR_EMPTY.validate("version",
				request.getVersion());
		DataValidators.NOT_NULL_OR_EMPTY.validate("bean name",
				request.getBeanName());
		DataValidators.NOT_NULL_OR_EMPTY.validate("bean definition",
				request.getBeanDefinition());
		DataValidators.NOT_NULL_OR_EMPTY.validate("scope", request.getScope());

		DataValidators.IS_VALID_SCOPE.validate("scope", request.getScope());

		String beanDefinition = cmgXMLParser
				.prettyFormat(getBeanDefinition(request.getBeanDefinition()));
		request.setBeanDefinition(beanDefinition);
		DataValidators.IS_VALID_BEAN_DEFINITION.validate("bean definition",
				request.getBeanDefinition());
		request.setBeanName(cmgXMLParser.getId(beanDefinition));
	}

	@RequestMapping(value = "/create", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult create(@RequestBody JetStreamBeanConfigurationDo request,
			HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
		try {
			createOneBean(request, httpRequest);
			return new ResponseResult(true);
		} catch (Exception ex) {
			return new ResponseResult(false, "create failed due to: "
					+ ex.getMessage());
		}
	}

	@RequestMapping(value = "/batchCreate", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult batchCreate(
			@RequestBody JetStreamBeanConfigurationDo[] request,
			HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
		if (request.length < 1)
			return new ResponseResult(false, "Please choose at least one bean.");
		for (JetStreamBeanConfigurationDo config : request) {
			try {
				createOneBean(config, httpRequest);
			} catch (Exception ex) {
				return new ResponseResult(false, "create "
						+ config.getBeanName() + " failed due to: "
						+ ex.getMessage());
			}
		}
		return new ResponseResult(true);
	}

	@RequestMapping(value = "/batchUpdate", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult batchUpdate(
			@RequestBody JetStreamBeanConfigurationDo[] request,
			HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
		if (request.length < 1)
			return new ResponseResult(false, "Please choose at least one bean.");
		for (JetStreamBeanConfigurationDo config : request) {
			try {
				updateOneBean(config, httpRequest);
			} catch (Exception ex) {
				return new ResponseResult(false, "update "
						+ config.getBeanName() + " failed due to: "
						+ ex.getMessage());
			}
		}
		return new ResponseResult(true);
	}

	private void createOneBean(JetStreamBeanConfigurationDo config,
			HttpServletRequest httpRequest) throws Exception {
		validateRequest(config);
		checkDuplicateBean(config);
		String beanVersion = config.getBeanVersion();
		if (beanVersion == null || beanVersion.isEmpty()) {
			beanVersion = Integer.valueOf(1).toString();
		}
		config.setBeanVersion(beanVersion);
		config.setCreatedBy(getLoginUser(httpRequest));
		long time = System.currentTimeMillis();
		config.setCreationDate(time);
		config.setModifiedDate(time);
		mongoConfigManager.uploadJetStreamConfiguration(config);
		configMessagePublisher.publish(config.getAppName(), config.getScope(),
				config.getVersion(), config.getBeanName(),
				config.getBeanVersion());
	}

	@RequestMapping(value = "/validate", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult validate(@RequestBody JetStreamBeanConfigurationDo[] request) {
		if (request.length < 1)
			return new ResponseResult(false, "Please choose at least one bean.");
		for (JetStreamBeanConfigurationDo config : request) {
			try {
				validateRequest(config);
			} catch (Exception ex) {
				return new ResponseResult(false, "validate "
						+ config.getBeanName() + " failed due to "
						+ ex.getMessage());
			}
		}
		return new ResponseResult(true);
	}

	@RequestMapping(value = "/validateFile", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult validateFile(
			@RequestBody JetStreamBeanConfigurationDo request) {
		try {
			DataValidators.NOT_NULL_OR_EMPTY.validate("app name",
					request.getAppName());
			DataValidators.NOT_NULL_OR_EMPTY.validate("version",
					request.getVersion());
			DataValidators.NOT_NULL_OR_EMPTY.validate("file content",
					request.getBeanDefinition());
			DataValidators.IS_VALID_BEAN_DEFINITION.validate("file content",
					URLDecoder.decode(request.getBeanDefinition(), "UTF-8"));
		} catch (Exception ex) {
			return new ResponseResult(false, ex.getMessage());
		}
		return new ResponseResult(true);
	}

	@RequestMapping(value = "/update", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult update(@RequestBody JetStreamBeanConfigurationDo request,
			HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
		try {
			validateRequest(request);
			boolean result = mongoConfigManager.removeJetStreamConfiguration(
					request.getAppName(), request.getVersion(),
					request.getBeanName(), request.getBeanVersion(),
					request.getScope());
			if (!result) {
				return new ResponseResult(false, "update failed.");
			}

			int currentVersion = Integer.valueOf(request.getBeanVersion());
			String beanVer = String.valueOf(currentVersion + 1);
			request.setBeanVersion(beanVer);
			request.setModifiedBy(getLoginUser(httpRequest));
			request.setModifiedDate(System.currentTimeMillis());
			mongoConfigManager.uploadJetStreamConfiguration(request);
			configMessagePublisher.publish(request.getAppName(),
					request.getScope(), request.getVersion(),
					request.getBeanName(), beanVer);

		} catch (Exception ex) {
			return new ResponseResult(false, "update failed due to: "
					+ ex.getMessage());
		}

		return new ResponseResult(true);
	}

	private void updateOneBean(JetStreamBeanConfigurationDo config,
			HttpServletRequest httpRequest) throws Exception {
		validateRequest(config);
		String updateType = httpRequest.getParameter("type");
		if ("update".equalsIgnoreCase(updateType)) {
			updateBeanRequest(config, httpRequest);
		} else if ("rollback".equalsIgnoreCase(updateType)) {
			rollbackBeanRequest(config, httpRequest);
		}
	}

	private void updateBeanRequest(JetStreamBeanConfigurationDo config,
			HttpServletRequest httpRequest) throws Exception {
		JetStreamBeanConfigurationDo oldConfig = mongoConfigManager
				.getJetStreamConfiguration(config.getAppName(),
						config.getVersion(), config.getBeanName(),
						config.getBeanVersion(), config.getScope()).get(0);
		JetStreamBeanConfigurationLogDo logConfigBean = createLogBeanWithStatus(
				oldConfig, httpRequest, UPDATED_STATUS);
		try {
			mongoConfigLogManager.uploadJetStreamConfiguration(logConfigBean);
		} catch (Exception ex) {

			throw new Exception(
					"Meet execption while update bean log, exception = "
							+ ex.getMessage());
		}

		try {
			boolean result = mongoConfigManager.removeJetStreamConfiguration(
					config.getAppName(), config.getVersion(),
					config.getBeanName(), config.getBeanVersion(),
					config.getScope());
			if (!result) {
				throw new Exception("Cannot delete bean.");
			}
		} catch (Exception ex) {

			throw new Exception(
					"Meet exception while delete bean, exception = "
							+ ex.getMessage());
		}

		int currentVersion = Integer.valueOf(config.getBeanVersion());
		String beanVer = String.valueOf(currentVersion + 1);
		config.setBeanVersion(beanVer);
		config.setCreatedBy(oldConfig.getCreatedBy());
		config.setCreationDate(oldConfig.getCreationDate());
		config.setModifiedBy(getLoginUser(httpRequest));
		config.setModifiedDate(System.currentTimeMillis());
		try {
			mongoConfigManager.uploadJetStreamConfiguration(config);

		} catch (Exception ex) {

			throw new Exception(
					"Meet exception while update bean, you could rollback from bean log. exception = "
							+ ex.getMessage());
		}
		configMessagePublisher.publish(config.getAppName(), config.getScope(),
				config.getVersion(), config.getBeanName(), beanVer);

	}

	private void rollbackBeanRequest(JetStreamBeanConfigurationDo config,
			HttpServletRequest httpRequest) throws Exception {
		JetStreamBeanConfigurationLogDo toBePushConfig = mongoConfigLogManager
				.getJetStreamConfiguration(config.getAppName(),
						config.getVersion(), config.getBeanName(),
						config.getBeanVersion(), config.getScope()).get(0);

		int maxBenVerInConfigTable = mongoConfigManager.getMaxBeanVersion(
				config.getAppName(), config.getVersion(), config.getBeanName(),
				config.getScope());
		if (maxBenVerInConfigTable != -1) {
			JetStreamBeanConfigurationDo toBeReplace = mongoConfigManager
					.getJetStreamConfiguration(config.getAppName(),
							config.getVersion(), config.getBeanName(),
							String.valueOf(maxBenVerInConfigTable),
							config.getScope()).get(0);

			boolean result = mongoConfigManager.removeJetStreamConfiguration(
					config.getAppName(), config.getVersion(),
					config.getBeanName(),
					String.valueOf(maxBenVerInConfigTable), config.getScope());
			if (!result) {
				throw new Exception("Cannot delete bean.");
			}
			JetStreamBeanConfigurationLogDo logConfigBean = createLogBeanWithStatus(
					toBeReplace, httpRequest, UPDATED_STATUS);
			mongoConfigLogManager.uploadJetStreamConfiguration(logConfigBean);
		}

		String beanVer = String.valueOf(getMaxBeanVersion(config) + 1);
		String loginUser = getLoginUser(httpRequest);
		config.setBeanVersion(beanVer);
		config.setCreatedBy(toBePushConfig == null ? loginUser : toBePushConfig
				.getCreatedBy());
		config.setCreationDate(toBePushConfig.getCreationDate());
		config.setModifiedBy(loginUser);
		config.setModifiedDate(System.currentTimeMillis());
		mongoConfigManager.uploadJetStreamConfiguration(config);
		configMessagePublisher.publish(config.getAppName(), config.getScope(),
				config.getVersion(), config.getBeanName(), beanVer);

	}

	private int getMaxBeanVersion(JetStreamBeanConfigurationDo config) {
		int maxBeanVersion = mongoConfigManager.getMaxBeanVersion(
				config.getAppName(), config.getVersion(), config.getBeanName(),
				config.getScope());
		if (maxBeanVersion == -1) {
			maxBeanVersion = mongoConfigLogManager.getMaxBeanVersion(
					config.getAppName(), config.getVersion(),
					config.getBeanName(), config.getScope());
		}
		if (maxBeanVersion == -1) {
			maxBeanVersion = 0;
		}
		return maxBeanVersion;
	}

	private JetStreamBeanConfigurationLogDo createLogBeanWithStatus(
			JetStreamBeanConfigurationDo oldConfig,
			HttpServletRequest httpRequest, int status) {
		JetStreamBeanConfigurationLogDo logConfig = new JetStreamBeanConfigurationLogDo();
		logConfig.setAppName(oldConfig.getAppName());
		logConfig.setBeanDefinition(oldConfig.getBeanDefinition());
		logConfig.setBeanName(oldConfig.getBeanName());
		logConfig.setBeanVersion(oldConfig.getBeanVersion());
		logConfig.setCreatedBy(oldConfig.getCreatedBy());
		logConfig.setCreationDate(oldConfig.getCreationDate());
		logConfig.setModifiedBy(oldConfig.getModifiedBy());
		logConfig.setModifiedDate(oldConfig.getModifiedDate());
		logConfig.setOperatedBy(getLoginUser(httpRequest));
		logConfig.setOperatedDate(System.currentTimeMillis());
		logConfig.setScope(oldConfig.getScope());
		logConfig.setStatus(status);
		logConfig.setVersion(oldConfig.getVersion());
		return logConfig;

	}

	@RequestMapping(value = "/batchDelete", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult batchDelete(
			@RequestBody JetStreamBeanConfigurationDo[] request,
			HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
		if (request.length < 1)
			return new ResponseResult(false, "Please choose at least one bean.");
		for (JetStreamBeanConfigurationDo config : request) {
			try {
				deleteOneBean(config, httpRequest);
			} catch (Exception ex) {
				return new ResponseResult(false, "Delete "
						+ config.getAppName() + " failed due to: "
						+ ex.getMessage());
			}
		}
		return new ResponseResult(true);
	}

	private void deleteOneBean(JetStreamBeanConfigurationDo config,
			HttpServletRequest httpRequest) throws Exception {
		JetStreamBeanConfigurationDo oldConfig = mongoConfigManager
				.getJetStreamConfiguration(config.getAppName(),
						config.getVersion(), config.getBeanName(),
						config.getBeanVersion(), config.getScope()).get(0);
		boolean result = mongoConfigManager.removeJetStreamConfiguration(
				config.getAppName(), config.getVersion(), config.getBeanName(),
				config.getBeanVersion(), config.getScope());
		if (!result) {
			throw new Exception("Cannot delete bean");
		}

		JetStreamBeanConfigurationLogDo logConfigBean = createLogBeanWithStatus(
				oldConfig, httpRequest, DELETED_STATUS);
		mongoConfigLogManager.uploadJetStreamConfiguration(logConfigBean);
	}

	protected String getLoginUser(HttpServletRequest httpRequest) {
		return httpRequest.getRemoteUser();
	}

	@RequestMapping(value = "/parseBeans", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody
	ResponseResult parseBeans(HttpServletRequest request,
			HttpServletResponse response) {
		try {
			String source = request.getParameter("beanDefinition");
			DataValidators.NOT_NULL_OR_EMPTY.validate("file content", source);
			DataValidators.IS_VALID_BEAN_DEFINITION.validate("bean definition",
					source);
			List<JetStreamBeanConfigurationDo> list = cmgXMLParser
					.parse(request.getParameter("beanDefinition"));
			return new ResponseResult(true, null, list);
		} catch (Exception ex) {
			return new ResponseResult(false, "upload failed due to: "
					+ ex.getMessage());
		}
	}
}