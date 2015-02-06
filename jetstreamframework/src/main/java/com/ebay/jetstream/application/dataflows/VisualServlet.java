/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.application.dataflows;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.ebay.jetstream.config.RootConfiguration;

public class VisualServlet extends HttpServlet {

	@Override
	public void init() throws ServletException {
		super.init();
	}

	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		VisualDataFlow visual = (VisualDataFlow) RootConfiguration
				.get("VisualDataFlow");
		ByteBuffer content = visual.getVisual();
		response.setContentType("image/gif");
		response.setContentLength(content.capacity());
		response.getOutputStream().write(content.array());
	}

}
