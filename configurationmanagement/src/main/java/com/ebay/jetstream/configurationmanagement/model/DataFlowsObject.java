/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.model;

import java.util.Map;
import java.util.Set;

public class DataFlowsObject {
	private String bean;
	private String description;
	private Map<String, Set<String>> graph;
	private Set<String> nodes;

	public String getBean() {
		return bean;
	}

	public void setBean(String bean) {
		this.bean = bean;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Map<String, Set<String>> getMap() {
		return graph;
	}

	public void setMap(Map<String, Set<String>> graph) {
		this.graph = graph;
	}
	
	public Set<String> getNodes() {
		return graph.keySet();
	}
	
	public void setNodes(Set<String> nodes) {
		this.nodes = nodes;
	}

}
 