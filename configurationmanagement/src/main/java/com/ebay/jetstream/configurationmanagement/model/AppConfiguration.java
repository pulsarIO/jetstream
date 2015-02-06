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
