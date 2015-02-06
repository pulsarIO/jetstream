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

import java.util.Date;

import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;

public class JetStreamBeanConfiguration extends JetStreamBeanConfigurationDo {

	private String creationDateAsString;
	private String modifiedDateAsString;

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

	public static JetStreamBeanConfiguration convert(
			JetStreamBeanConfigurationDo doObj) {
		JetStreamBeanConfiguration toObj = new JetStreamBeanConfiguration();
		toObj.setAppName(doObj.getAppName());
		String beanDefinition = doObj.getBeanDefinition();
		int start = beanDefinition.indexOf("<bean ");
		int end = beanDefinition.lastIndexOf("</bean>");
		if (start != -1 && end != -1) {
			beanDefinition = beanDefinition.substring(start,
					end + "</bean>".length());
		}else{
			end = beanDefinition.lastIndexOf("/>");
			if(start != -1 && end != -1){
				beanDefinition = beanDefinition.substring(start,
						end + "/>".length());
			}
		}
		toObj.setBeanDefinition(beanDefinition);
		toObj.setBeanName(doObj.getBeanName());
		toObj.setBeanVersion(doObj.getBeanVersion());
		toObj.setCreatedBy(doObj.getCreatedBy());
		toObj.setCreationDate(doObj.getCreationDate());
		if(doObj.getCreationDate() > 0)
			toObj.setCreationDateAsString(new Date(doObj.getCreationDate())
				.toString());
		else
			toObj.setCreationDateAsString("");
		toObj.setModifiedBy(doObj.getModifiedBy());
		toObj.setModifiedDate(doObj.getModifiedDate());
		if(doObj.getModifiedDate() > 0)
			toObj.setModifiedDateAsString(new Date(doObj.getModifiedDate())
				.toString());
		else
			toObj.setModifiedDateAsString("");
		toObj.setScope(doObj.getScope());
		toObj.setVersion(doObj.getVersion());

		return toObj;
	}
}
