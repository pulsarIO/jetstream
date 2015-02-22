/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement;



import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import com.ebay.jetstream.config.AbstractNamedBean;

public class SpringContainerHolder  extends AbstractNamedBean implements BeanFactoryAware {
    private static DefaultListableBeanFactory beanFactory;

    public SpringContainerHolder() {
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        SpringContainerHolder.beanFactory = (DefaultListableBeanFactory) beanFactory;
    }

    public static<T> T getSpringBean(String name) {
        return (T) beanFactory.getBean(name);
    }

    public static<T> T  getSpringBean(Class<?> clazz) {
        return (T) beanFactory.getBean(clazz);
    }
}
