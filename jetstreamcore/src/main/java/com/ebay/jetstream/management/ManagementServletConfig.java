/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.management;

import org.springframework.beans.factory.InitializingBean;

public class ManagementServletConfig implements InitializingBean{
	
	private int threadpoolsize = 10;

	public int getThreadpoolsize() {
		return threadpoolsize;
	}

	public void setThreadpoolsize(int threadpoolsize) {
		this.threadpoolsize = threadpoolsize;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		
		
	}
	

}
