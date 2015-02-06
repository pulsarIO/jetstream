/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/

package com.ebay.jetstream.spring.context.support;

import java.io.IOException;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.ApplicationListener;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.ebay.jetstream.spring.beans.factory.support.UpdateableListableBeanFactory;

/**
 * Base class for {@link org.springframework.context.ApplicationContext} implementations which are supposed to support
 * multiple refreshs, creating a new internal bean factory instance every time. Typically (but not necessarily), such a
 * context will be driven by a set of config locations to load bean definitions from.
 * 
 * <p>
 * The only method to be implemented by subclasses is {@link #loadBeanDefinitions}, which gets invoked on each refresh.
 * A concrete implementation is supposed to load bean definitions into the given
 * {@link org.springframework.beans.factory.support.DefaultListableBeanFactory}, typically delegating to one or more
 * specific bean definition readers.
 * 
 * <p>
 * <b>Note that there is a similar base class for WebApplicationContexts.</b>
 * {@link org.springframework.web.context.support.AbstractRefreshableWebApplicationContext} provides the same
 * subclassing strategy, but additionally pre-implements all context functionality for web environments. There is also a
 * pre-defined way to receive config locations for a web context.
 * 
 * <p>
 * Concrete standalone subclasses of this base class, reading in a specific bean definition format, are
 * {@link ClassPathXmlApplicationContext} and {@link FileSystemXmlApplicationContext}, which both derive from the common
 * {@link AbstractXmlApplicationContext} base class.
 * 
 * @author Juergen Hoeller
 * @since 1.1.3
 * @see #loadBeanDefinitions
 * @see org.springframework.beans.factory.support.DefaultListableBeanFactory
 * @see org.springframework.web.context.support.AbstractRefreshableWebApplicationContext
 * @see AbstractXmlApplicationContext
 * @see ClassPathXmlApplicationContext
 * @see FileSystemXmlApplicationContext
 * 
 *      JETSTREAM 06-30-2008: renamed from AbstractRefreshableApplicationContext to Custom..., renamed member variables to
 *      eliminate warnings, and added locking support for re-creating individual beans in the container. 08-21-2008:
 *      renamed again to AbstractUpdateableApplicationContext and added support for BeanChangeAwareness via an extension
 *      to DefaultListableBeanFactory
 */
public abstract class AbstractUpdateableApplicationContext extends AbstractApplicationContext {
  private Boolean m_allowBeanDefinitionOverriding;
  private Boolean m_allowCircularReferences;
  /** Bean factory for this context */
  private UpdateableListableBeanFactory m_beanFactory;

  /** Synchronization monitor for the internal BeanFactory */
  private final Object m_beanFactoryMonitor = new Object();

  /**
   * Create a new AbstractUpdateableApplicationContext with no parent.
   */
  public AbstractUpdateableApplicationContext() {
  }

  /**
   * Create a new AbstractUpdateableApplicationContext with the given parent context.
   * 
   * @param parent
   *          the parent context
   */
  public AbstractUpdateableApplicationContext(ApplicationContext parent) {
    super(parent);
  }

  @Override
  public void addApplicationListener(ApplicationListener listener) {
    super.addApplicationListener(listener);
  }

  @Override
  protected final void closeBeanFactory() {
    synchronized (m_beanFactoryMonitor) {
      m_beanFactory = null;
    }
  }

  /**
   * Create an internal bean factory for this context. Called for each {@link #refresh()} attempt.
   * <p>
   * The default implementation creates a {@link org.springframework.beans.factory.support.DefaultListableBeanFactory}
   * with the {@link #getInternalParentBeanFactory() internal bean factory} of this context's parent as parent bean
   * factory. Can be overridden in subclasses, for example to customize DefaultListableBeanFactory's settings.
   * 
   * @return the bean factory for this context
   * @see org.springframework.beans.factory.support.DefaultListableBeanFactory#setAllowBeanDefinitionOverriding
   * @see org.springframework.beans.factory.support.DefaultListableBeanFactory#setAllowEagerClassLoading
   * @see org.springframework.beans.factory.support.DefaultListableBeanFactory#setAllowCircularReferences
   * @see org.springframework.beans.factory.support.DefaultListableBeanFactory#setAllowRawInjectionDespiteWrapping
   */
  protected UpdateableListableBeanFactory createBeanFactory() {
    return new UpdateableListableBeanFactory(getInternalParentBeanFactory());
  }

  /**
   * Customize the internal bean factory used by this context. Called for each {@link #refresh()} attempt.
   * <p>
   * The default implementation applies this context's {@link #setAllowBeanDefinitionOverriding
   * "m_allowBeanDefinitionOverriding"} and {@link #setAllowCircularReferences "m_allowCircularReferences"} settings, if
   * specified. Can be overridden in subclasses to customize any of {@link DefaultListableBeanFactory}'s settings.
   * 
   * @param m_beanFactory
   *          the newly created bean factory for this context
   * @see DefaultListableBeanFactory#setAllowBeanDefinitionOverriding
   * @see DefaultListableBeanFactory#setAllowCircularReferences
   * @see DefaultListableBeanFactory#setAllowRawInjectionDespiteWrapping
   * @see DefaultListableBeanFactory#setAllowEagerClassLoading
   */
  protected void customizeBeanFactory(DefaultListableBeanFactory beanFactory) {
    if (m_allowBeanDefinitionOverriding != null) {
      beanFactory.setAllowBeanDefinitionOverriding(m_allowBeanDefinitionOverriding.booleanValue());
    }
    if (m_allowCircularReferences != null) {
      beanFactory.setAllowCircularReferences(m_allowCircularReferences.booleanValue());
    }
  }

  @Override
  public final ConfigurableListableBeanFactory getBeanFactory() {
    UpdateableListableBeanFactory beanFactory = null;
    synchronized (m_beanFactoryMonitor) {
      beanFactory = m_beanFactory;
      if (beanFactory == null) {
        throw new IllegalStateException("BeanFactory not initialized or already closed - "
            + "call 'refresh' before accessing beans via the ApplicationContext");
      }
    }
    return beanFactory.waitForUpdates();
  }

  /**
   * Determine whether this context currently holds a bean factory, i.e. has been refreshed at least once and not been
   * closed yet.
   */
  protected final boolean hasBeanFactory() {
    synchronized (m_beanFactoryMonitor) {
      return m_beanFactory != null;
    }
  }

  /**
   * Load bean definitions into the given bean factory, typically through delegating to one or more bean definition
   * readers.
   * 
   * @param m_beanFactory
   *          the bean factory to load bean definitions into
   * @throws IOException
   *           if loading of bean definition files failed
   * @throws BeansException
   *           if parsing of the bean definitions failed
   * @see org.springframework.beans.factory.support.PropertiesBeanDefinitionReader
   * @see org.springframework.beans.factory.xml.XmlBeanDefinitionReader
   */
  protected abstract void loadBeanDefinitions(UpdateableListableBeanFactory beanFactory) throws IOException,
      BeansException;

  /**
   * This implementation performs an actual refresh of this context's underlying bean factory, shutting down the
   * previous bean factory (if any) and initializing a fresh bean factory for the next phase of the context's lifecycle.
   */
  @Override
  protected final void refreshBeanFactory() throws BeansException {
    if (hasBeanFactory()) {
      destroyBeans();
      closeBeanFactory();
    }
    try {
      UpdateableListableBeanFactory beanFactory = createBeanFactory();
      customizeBeanFactory(beanFactory);
      loadBeanDefinitions(beanFactory);
      synchronized (m_beanFactoryMonitor) {
        m_beanFactory = beanFactory;
      }
    }
    catch (IOException ex) {
      throw new ApplicationContextException("I/O error parsing XML document for application context ["
          + getDisplayName() + "]", ex);
    }
  }

  /**
   * Set whether it should be allowed to override bean definitions by registering a different definition with the same
   * name, automatically replacing the former. If not, an exception will be thrown. Default is "true".
   * 
   * @see org.springframework.beans.factory.support.DefaultListableBeanFactory#setAllowBeanDefinitionOverriding
   */
  public void setAllowBeanDefinitionOverriding(boolean allowBeanDefinitionOverriding) {
    m_allowBeanDefinitionOverriding = Boolean.valueOf(allowBeanDefinitionOverriding);
  }

  /**
   * Set whether to allow circular references between beans - and automatically try to resolve them.
   * <p>
   * Default is "true". Turn this off to throw an exception when encountering a circular reference, disallowing them
   * completely.
   * 
   * @see org.springframework.beans.factory.support.DefaultListableBeanFactory#setAllowCircularReferences
   */
  public void setAllowCircularReferences(boolean allowCircularReferences) {
    m_allowCircularReferences = Boolean.valueOf(allowCircularReferences);
  }

}
