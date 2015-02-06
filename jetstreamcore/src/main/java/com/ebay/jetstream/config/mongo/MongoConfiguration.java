/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.util.ArrayList;
import java.util.List;

public class MongoConfiguration {
	private List<String> hosts = new ArrayList<String>();
	private String db = null;
	private String user = null;
	private String pw = null;
	private String port = null;
	
	public List<String> getHosts() {
		return hosts;
	}

	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}
	
	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getPw() {
		return pw;
	}

	public void setPw(String pw) {
		this.pw = pw;
	}
	
	public String toString() {
		StringBuffer s = new StringBuffer();
		s.append("-hosts:").append(hosts);
		s.append("-db:").append(db);
		s.append("-port:").append(port);
		
		return s.toString();
		
	}

	
}
