/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.util.Date;

import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;

public class MongoConfigDetails implements XSerializable {
	private String beanName;
	private Date updateTime;
	private String beanVersion;
	private String user;
	private String application;
	private String applicationVersion;
	private String log = "";
	
	@Hidden
	public String getBeanName() {
		return beanName;
	}
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}
	@Hidden
	public Date getUpdateTime() {
		return updateTime;
	}
	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
	@Hidden
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	@Hidden
	public String getApplication() {
		return application;
	}
	public void setApplication(String application) {
		this.application = application;
	}
	@Hidden
	public String getApplicationVersion() {
		return applicationVersion;
	}
	public void setApplicationVersion(String applicationVersion) {
		this.applicationVersion = applicationVersion;
	}
	@Hidden
	public String getBeanVersion() {
		return beanVersion;
	}
	public void setBeanVersion(String beanVersion) {
		this.beanVersion = beanVersion;
	}
	@Hidden
	public String getLog() {
		return log;
	}
	public void setLog(String log) {
		this.log = log;
	}
	
	@Hidden
	public boolean equals(Object obj) {
		if (obj == this)
			return true;

		if (obj == null)
			return false;

		if (!(obj instanceof MongoConfigDetails))
			return false;

		if ( this.beanName.equals(((MongoConfigDetails) obj).beanName) &&
				this.beanVersion.equals(((MongoConfigDetails) obj).beanVersion) &&
				this.application.equals(((MongoConfigDetails) obj).application) && 
				this.applicationVersion.equals(((MongoConfigDetails) obj).applicationVersion) )
			return true;
		else
			return false;

	}

	@Hidden
	public int hashCode() { 
	    int hash = 1;
	    hash = hash * 31 
	    			+ (beanName == null ? 0 : beanName.hashCode());
	    hash = hash * 31 
					+ (beanVersion == null ? 0 : beanVersion.hashCode());
	    hash = hash * 31 
	                + (application == null ? 0 : application.hashCode());
	    	    hash = hash * 31 
        			+ (applicationVersion == null ? 0 : applicationVersion.hashCode());
	    
	    return hash;
	}
	
	public String getConfigDetails() {
		return toString();
	}
	
	public String toString() {
		return " [beanName = " + beanName + ", beanVersion = " + beanVersion+ ", application = " + application
		+ ", applicationVersion = " + applicationVersion 
		+ ", modifiedDate = " + updateTime 
		+ ", user = " + user + "]" + log;
	}
	
}
