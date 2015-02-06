/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

public class BeanInformation {
	private String appName;
	private String version;
	private String beanVersion;
	private String scope;
	private String beanName;
	
	public BeanInformation(String appName, String version, String beanVersion, String scope, String beanName) {
		this.appName = appName;
		this.version = version;
		this.beanVersion = beanVersion;
		this.scope = scope;
		this.beanName = beanName;
	}
	
	public String getAppName() {
		return appName;
	}
	public void setAppName(String appName) {
		this.appName = appName;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getBeanVersion() {
		return beanVersion;
	}
	public void setBeanVersion(String beanVersion) {
		this.beanVersion = beanVersion;
	}
	public String getScope() {
		return scope;
	}
	public void setScope(String scope) {
		this.scope = scope;
	}
	public String getBeanName() {
		return beanName;
	}
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}
	
	@Override
	public String toString() {
		return "appName=" + appName
				+ ", version=" + version 
				+ ", beanName=" + beanName 
				+ ", beanVersion=" + beanVersion
				+ ", scope=" + scope ;
	}

}