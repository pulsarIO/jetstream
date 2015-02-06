/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jestream.event.annotation;

import com.ebay.jetstream.event.JetstreamEvent;

public interface AnnotationListener {
	
	public JetstreamEvent processMetaInformation(JetstreamEvent event,
			StatementAnnotationInfo annotationInfo);
	
	public Class getAnnotationClass();

}
