/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.model;

import java.util.ArrayList;
import java.util.List;

import com.ebay.jetstream.configurationmanagement.JsonUtil;

public class AppConfiguration implements Comparable<AppConfiguration>{
	private String name;
	private String machine;
	private int port;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMachine() {
		return machine;
	}

	public void setMachine(String machine) {
		this.machine = machine;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof AppConfiguration) {
			AppConfiguration appConfig = (AppConfiguration) obj;
			return getName().equals(appConfig.getName());
		}

		return false;
	}

	public int hashCode() {
		return getName().hashCode();
	}


	@Override
	public int compareTo(AppConfiguration o) {
		return getName().compareTo(o.getName());
	}
}
