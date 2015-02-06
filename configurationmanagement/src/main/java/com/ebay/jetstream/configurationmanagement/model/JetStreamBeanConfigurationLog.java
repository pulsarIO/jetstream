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
