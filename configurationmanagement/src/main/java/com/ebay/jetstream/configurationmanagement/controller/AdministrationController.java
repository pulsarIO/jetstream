/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.controller;

import java.text.ParseException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/administration")
public class AdministrationController extends AbstractController {
	@RequestMapping
	public String entry(HttpServletRequest request, HttpServletResponse response) {
		return "administration";
	}

	@RequestMapping("/purgeBeanLog")
	@ResponseBody
	public void purgeBeanLog(HttpServletRequest request) throws ParseException {

		String purgeDate = request.getParameter("purgeDate");
		mongoConfigLogManager.removeBeanLogBefore(purgeDate);
		// System.out.println(purgeDate);
	}
}
