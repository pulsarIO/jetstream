/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;


public class JetStreamBeanConfigurationDo {
	private String appName;
	private String version;
	private String beanDefinition;
	private String beanName;
	private String beanVersion;
	private String scope;
	private String createdBy;
	private String modifiedBy;
	private long creationDate;
	private long modifiedDate;
	
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
	public String getBeanDefinition() {
		return beanDefinition;
	}
	public void setBeanDefinition(String beanDefinition) {
		this.beanDefinition = beanDefinition;
	}
	public String getBeanName() {
		return beanName;
	}
	public void setBeanName(String beanName) {
		this.beanName = beanName;
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
	public String getCreatedBy() {
		return createdBy;
	}
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
	public String getModifiedBy() {
		return modifiedBy;
	}
	public void setModifiedBy(String modifiedBy) {
		this.modifiedBy = modifiedBy;
	}
	public long getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}
	public long getModifiedDate() {
		return modifiedDate;
	}
	public void setModifiedDate(long modifiedDate) {
		this.modifiedDate = modifiedDate;
	}
	@Override
	public String toString() {
		return "JetStreamBeanConfigurationDo [appName=" + appName
				+ ", version=" + version + ", beanDefinition=" + beanDefinition
				+ ", beanName=" + beanName + ", beanVersion=" + beanVersion
				+ ", scope=" + scope + ", createdBy=" + createdBy
				+ ", modifiedBy=" + modifiedBy + ", creationDate="
				+ creationDate + ", modifiedDate=" + modifiedDate + "]";
	}
	
	public BeanInformation getBeanInformation() {
		return (new BeanInformation(appName, version, beanVersion, scope, beanName));
	}

}