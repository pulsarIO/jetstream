/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.application.dataflows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.config.RootConfiguration;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.util.CommonUtils;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * Visualizes the Jetstream pipeline using yuml.me
 * 
 * @author varavindan
 * 
 */
@ManagedResource(objectName = "Event/DataFlows", description = "data flow visualization")
public class VisualDataFlow extends AbstractNamedBean implements XSerializable,
		InitializingBean {

	private Map<String, Set<String>> graph;
	private DefaultListableBeanFactory beanFactory;
	private static final String yumlUrl = "http://yuml.me/";
	private static final String yumlActivityUrl = "http://yuml.me/diagram/scruffy/activity";
	private static final int BUF = 512;
	private final Map<String, URL> graphYuml = new HashMap<String, URL>();
	private static Logger logger = LoggerFactory.getLogger(VisualDataFlow.class); 

	public Map<String, URL> getGraphYuml() {
		return graphYuml;
	}

	public VisualDataFlow() {
	}

	private URL getYumlPic(String yumlData) {
		try {
			if (!graphYuml.containsKey(yumlData)) {
				HttpClient httpclient = new HttpClient();
				PostMethod post = new PostMethod(yumlActivityUrl);
				List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
				post.addParameter(new NameValuePair("dsl_text", yumlData));
				httpclient.executeMethod(post);
				URL url = new URL(yumlUrl + post.getResponseBodyAsString());
				graphYuml.put(yumlData, url);
				return url;
			} else {
				return graphYuml.get(yumlData);
			}
		} catch (Exception e) {
			logger.error(" Exception while rendering pipeline ", e);
			return null;
		}
	}

	public ByteBuffer getAsByteArray(URL url) throws IOException {
		URLConnection connection = url.openConnection();
		InputStream in = connection.getInputStream();
		int contentLength = connection.getContentLength();
		ByteArrayOutputStream tmpOut;

		if (contentLength != -1) {
			tmpOut = new ByteArrayOutputStream(contentLength);
		} else {
			tmpOut = new ByteArrayOutputStream(16384);
		}

		byte[] buf = new byte[BUF];
		while (true) {
			int len = in.read(buf);
			if (len == -1) {
				break;
			}
			tmpOut.write(buf, 0, len);
		}
		in.close();
		tmpOut.close();
		byte[] array = tmpOut.toByteArray();
		return ByteBuffer.wrap(array);
	}

	@Hidden
	public ByteBuffer getVisual() {
		try {
			String yumlData = "";
			DataFlows dFlow = (DataFlows) RootConfiguration.get("DataFlows");
			Map<String, Set<String>> graph = dFlow.getGraph();
			int edges = graph.size();
			for (Entry<String, Set<String>> edge : graph.entrySet()) {
				if (!CommonUtils.isEmpty(edge.getValue())) {
					for (String v : edge.getValue()) {
						yumlData += "(" + edge.getKey() + ")" + "->" + "(" + v
								+ ")";
						yumlData += ",";
					}
				} else {
					yumlData += "(" + edge.getKey() + ")";
					yumlData += ",";
				}
			}
			yumlData = yumlData.replaceAll(",$", "");
			return getAsByteArray(getYumlPic(yumlData));
		} catch (Throwable e) {
			logger.error(" Exception while construting pipeline data ", e);
			return null;
		}
	}

	public void setGraph(Map<String, Set<String>> graph) {
		this.graph = graph;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Management.addBean(getBeanName(), this);
	}

}
