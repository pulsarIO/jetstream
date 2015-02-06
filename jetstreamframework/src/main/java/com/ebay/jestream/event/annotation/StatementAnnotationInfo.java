/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jestream.event.annotation;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public final class StatementAnnotationInfo {

	private Map<String, Object> annotationInfoMap = new HashMap<String, Object>();
	private List<String> annotationInfoKeyList = new ArrayList<String>();

	public StatementAnnotationInfo() {

	}

	public void addAnnotationInfo(String annotationName,
			Object annotationMetaData) {
		annotationInfoMap.put(annotationName, annotationMetaData);
		annotationInfoKeyList.add(annotationName);
	}

	public Object getAnnotationInfo(String annotationName) {
		return annotationInfoMap.get(annotationName);
	}

	public Map<String, Object> getAnnotationInfoMap() {
		return annotationInfoMap;
	}

	public List<String> getAnnotationInfoKeyList() {
		return annotationInfoKeyList;
	}
}
