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
