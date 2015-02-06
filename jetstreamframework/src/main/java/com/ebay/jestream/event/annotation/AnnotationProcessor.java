/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jestream.event.annotation;

import java.util.Collection;
import java.util.Map;

import com.ebay.jetstream.event.EventSink;

public interface AnnotationProcessor {
	
	public Class getAnnotationClass();
	
	public StatementAnnotationInfo process(String statement,
			EngineMetadata engineMetadata,
			Map<String, AnnotationConfiguration> annotConfigMap,
			StatementAnnotationInfo stmtAnntInfo,
			Collection<EventSink> registeredSinkList);

}
