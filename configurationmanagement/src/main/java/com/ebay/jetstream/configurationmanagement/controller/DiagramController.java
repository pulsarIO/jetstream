/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.ebay.jetstream.configurationmanagement.DataFlowsUtls;
import com.ebay.jetstream.configurationmanagement.JsonUtil;
import com.ebay.jetstream.configurationmanagement.model.AppConfiguration;
import com.ebay.jetstream.configurationmanagement.model.DataFlowsObject;


@Controller
@RequestMapping(value = "/configuration/diagram")
public class DiagramController extends AbstractController{
	private static final String DATA = "data";

	@RequestMapping
	public String entry(HttpServletRequest httpRequest, @RequestParam("app") String app) {
		AppConfiguration appConfig = getAppConfigByApp(app);
		if (appConfig == null) {
			httpRequest.setAttribute(ERROR, "App is not configured");
			return "diagram";
		}

		try {
			DataFlowsObject dfObj = DataFlowsUtls.getDataFlow(appConfig.getMachine(), appConfig.getPort());
			dfObj.setNodes(dfObj.getNodes());
			httpRequest.setAttribute(DATA, JsonUtil.toJson(dfObj));
		} catch (Exception e) {
			httpRequest.setAttribute(ERROR, "Connect failed, please check whether you have set machine or pool, port correctly");
		}

		return "diagram";
	}
}
