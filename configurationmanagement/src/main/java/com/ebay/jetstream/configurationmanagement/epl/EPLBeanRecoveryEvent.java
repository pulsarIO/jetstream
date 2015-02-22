/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement.epl;

import org.springframework.beans.factory.NamedBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

public class EPLBeanRecoveryEvent extends ApplicationContextEvent {
	private static final long serialVersionUID = 1L;

	private final String m_beanName;

	private final Throwable m_throwable;

	public EPLBeanRecoveryEvent(ApplicationContext context, String beanName,
			Throwable t) {
		super(context);
		m_beanName = beanName;
		m_throwable = t;
	}

	public String getBeanName() {
		return m_beanName;
	}

	public Throwable getThrowable() {
		return m_throwable;
	}

	public Object getChangedBean() {
		return getApplicationContext().getBean(getBeanName());
	}

	public boolean isChangedBean(NamedBean bean) {
		return bean != null && getBeanName().equals(bean.getBeanName());
	}
}
