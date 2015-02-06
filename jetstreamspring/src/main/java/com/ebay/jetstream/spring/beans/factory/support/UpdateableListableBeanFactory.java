/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/

package com.ebay.jetstream.spring.beans.factory.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;

import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;

/**
 * 
 */
public class UpdateableListableBeanFactory extends DefaultListableBeanFactory {
  private final Lock m_updating = new ReentrantLock();
  private final Map<String, Object> m_changeAwareBeans = new HashMap<String, Object>();
  private final Collection<String> m_updatingBeans = new ArrayList<String>(1);
  private final Collection<String> m_dependentBeans = new ArrayList<String>();

  public UpdateableListableBeanFactory() {
    super();
  }

  /**
   * @param parentBeanFactory
   */
  public UpdateableListableBeanFactory(BeanFactory parentBeanFactory) {
    super(parentBeanFactory);
  }

  public void addBeanForUpdate(String beanName) {
    m_updating.lock();
    m_updatingBeans.add(beanName);
  }

  @Override
  public void destroySingleton(String beanName) {
    if (!isUpdating() || isUpdatingBean(beanName) || !isBeanChangeAware(beanName))
      super.destroySingleton(beanName);
    else
      // Bean is change aware but dependencies may need reregistration
      m_dependentBeans.add(beanName);
  }

  public boolean isUpdating() {
    return m_updatingBeans.size() > 0;
  }

  public boolean isUpdatingBean(String bean) {
    return m_updatingBeans.contains(bean);
  }

  public void updateComplete() {
    for (String dependent : m_dependentBeans)
      for (String dependency : getDependenciesForBean(dependent))
        if (!isBeanChangeAware(dependency))
          // NOTE: this is overkill but not harmful
          // we only need to reregister destroyed beans, but we don't track them all
          registerDependentBean(dependency, dependent);
    m_updatingBeans.clear();
    m_dependentBeans.clear();
    m_updating.unlock();
  }

  public UpdateableListableBeanFactory waitForUpdates() {
    m_updating.lock();
    m_updating.unlock();
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory#doCreateBean(java.lang.String,
   *      org.springframework.beans.factory.support.RootBeanDefinition, java.lang.Object[])
   */
  @Override
  protected Object doCreateBean(String beanName, RootBeanDefinition mbd, Object[] args) {
    Object bean = super.doCreateBean(beanName, mbd, args);
    if (mbd.isSingleton() && bean instanceof BeanChangeAware)
      m_changeAwareBeans.put(beanName, bean);

    return bean;
  }

  protected boolean isBeanChangeAware(String beanName) {
    return m_changeAwareBeans.containsKey(beanName);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory#removeSingleton(java.lang.String)
   */
  @Override
  protected void removeSingleton(String beanName) {
    super.removeSingleton(beanName);
    m_changeAwareBeans.remove(beanName);
  }
}
