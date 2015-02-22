/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class JetStreamBeanConfigurationLog extends
		JetStreamBeanConfigurationLogDo {
	private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

	private String creationDateAsString;
	private String modifiedDateAsString;
	private String operatedDateAsString;

	public String getCreationDateAsString() {
		return creationDateAsString;
	}

	public void setCreationDateAsString(String creationDateAsString) {
		this.creationDateAsString = creationDateAsString;
	}

	public String getModifiedDateAsString() {
		return modifiedDateAsString;
	}

	public void setModifiedDateAsString(String modifiedDateAsString) {
		this.modifiedDateAsString = modifiedDateAsString;
	}

	public String getOperatedDateAsString() {
		return operatedDateAsString;
	}

	public void setOperatedDateAsString(String operatedDateAsString) {
		this.operatedDateAsString = operatedDateAsString;
	}

	public static JetStreamBeanConfigurationLog convert(
			JetStreamBeanConfigurationLogDo doObj) {
		JetStreamBeanConfigurationLog toObj = new JetStreamBeanConfigurationLog();
		toObj.setAppName(doObj.getAppName());
		String beanDefinition = doObj.getBeanDefinition();
		int start = beanDefinition.indexOf("<bean ");
		int end = beanDefinition.lastIndexOf("</bean>");
		if (start != -1 && end != -1) {
			beanDefinition = beanDefinition.substring(start,
					end + "</bean>".length());
		} else {
			end = beanDefinition.lastIndexOf("/>");
			if (start != -1 && end != -1) {
				beanDefinition = beanDefinition.substring(start,
						end + "/>".length());
			}
		}
		toObj.setBeanDefinition(beanDefinition);
		toObj.setBeanName(doObj.getBeanName());
		toObj.setBeanVersion(doObj.getBeanVersion());
		toObj.setCreatedBy(doObj.getCreatedBy());
		toObj.setCreationDate(doObj.getCreationDate());
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
		if (doObj.getCreationDate() > 0)
			toObj.setCreationDateAsString(sdf.format(new Date(doObj
					.getCreationDate())));

		else
			toObj.setCreationDateAsString("");
		toObj.setModifiedBy(doObj.getModifiedBy());
		toObj.setModifiedDate(doObj.getModifiedDate());
		if (doObj.getModifiedDate() > 0)
			toObj.setModifiedDateAsString(sdf.format(new Date(doObj
					.getModifiedDate())));

		else
			toObj.setModifiedDateAsString("");

		if (doObj.getOperatedDate() > 0)
			toObj.setOperatedDateAsString(sdf.format(new Date(doObj
					.getOperatedDate())));
		else
			toObj.setOperatedDateAsString("");
		toObj.setScope(doObj.getScope());
		toObj.setVersion(doObj.getVersion());
		toObj.setStatus(doObj.getStatus());
		toObj.setOperatedBy(doObj.getOperatedBy());

		return toObj;
	}
}
