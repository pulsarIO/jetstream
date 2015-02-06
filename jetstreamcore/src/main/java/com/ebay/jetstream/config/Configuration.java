/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.config;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.AbstractBeanDefinitionReader;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import com.ebay.jetstream.config.mongo.MongoDbResource;
import com.ebay.jetstream.spring.beans.factory.support.UpdateableListableBeanFactory;
import com.ebay.jetstream.spring.context.support.AbstractUpdateableApplicationContext;
import com.ebay.jetstream.util.CommonUtils;

/**
 * 
 */
public class Configuration extends AbstractUpdateableApplicationContext {
  protected static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class.getName());

  /**
   * @param clazz
   *            the class whose package should be searched
   * @param name
   *            the name of the context container, or null. If null, the class name itself is used as container name.
   * @return the Spring form of the context container in the given class location.
   */
  public static String getClasspathContext(Class<?> clazz, String name) {
    String path = "classpath:" + clazz.getCanonicalName().replace('.', '/');
    return name == null ? path + ".xml" : path.substring(0, path.lastIndexOf('/') + 1) + name;
  }

  /**
   * Looks for classpath contexts matching the class name or any super class or interface.
   * 
   * @param clazz
   *            the class leaf to look for contexts
   * @return all files matching the class name but ending in .xml for all impls and interfaces
   */
  public static List<String> getClasspathContexts(Class<?> clazz) {
    List<String> names = new ArrayList<String>();
    for (Class<?> current = clazz; current != null; current = current.getSuperclass())
      checkClass(names, current);
    for (Class<?> current : clazz.getInterfaces())
      checkClass(names, current);
    return names;
  }

  /**
   * Returns all context files at the given path, across multiple data stores: files, svn/dav, etc.
   * 
   * @param path
   *            the directory path to look for context files.
   * @return all files ending in .xml at the given path.
   */
  public static List<String> getContexts(String path) {
    try {
      ConfigDataSource cds = ConfigUtils.getConfigDataSource(path);
      List<String> names = new ArrayList<String>(cds.getStreamLocations());
      for (int i = names.size() - 1; i >= 0; i--)
        if (!names.get(i).toUpperCase().endsWith(".XML"))
          names.remove(i);
      return names;
    }
    catch (Throwable t) {
      throw CommonUtils.runtimeException(t);
    }
  }
  
  public static List<String> getMongoContexts() {
    List<String> mongo = new ArrayList<String>();
    
    String mongoInfo = ConfigUtils.getPropOrEnv("MONGO_HOME");
    //if (!CommonUtils.isEmptyTrimmed(mongoInfo))
    if(mongoInfo != null && !CommonUtils.isEmptyTrimmed(mongoInfo)) {
    	mongo.add(mongoInfo);
    } else {
    	mongo.add("mongo:null");
    }
    	
    
    return mongo;
  }

  /**
   * Returns the ldap urls to get configuration information from. Currently only one ldap url is returned, and only if
   * the LDAPINFORMATION property or env var is populated.
   * 
   * @return the list of ldap urls
   */
  public static List<String> getLdapContexts(ApplicationInformation ai) {
    List<String> ldap = new ArrayList<String>();
    String ldapInfo = ConfigUtils.getPropOrEnv("LDAPINFORMATION");
    if (ldapInfo != null) {
      int indexOfAt = ldapInfo.indexOf('@');
      if (indexOfAt != -1 && ai.containsKey("ldapHostPort") && ldapInfo.charAt(indexOfAt + 1) == '/') {
        ldapInfo = ldapInfo.substring(0, indexOfAt + 1) + ai.get("ldapHostPort") + ldapInfo.substring(indexOfAt + 1);
      }
      else if (indexOfAt != -1 && !ai.containsKey("ldapHostPort") && ldapInfo.charAt(indexOfAt + 1) == '/') {
        throw new RuntimeException("Ldap host/port not set");
      }
      if (!ldapInfo.contains("application=") && ai.containsKey("applicationName"))
        ldapInfo = addToUrlEnd(ldapInfo, "application=" + ai.get("applicationName"));
      if (!ldapInfo.contains("version=") && ai.containsKey("configVersion"))
        ldapInfo = addToUrlEnd(ldapInfo, "version=" + ai.get("configVersion"));
      if (!ldapInfo.contains("scope="))
        ldapInfo = addToUrlEnd(ldapInfo, "scope=" + ai.get("scope"));
    }
    if (!CommonUtils.isEmptyTrimmed(ldapInfo))
      ldap.add(ldapInfo);
    return ldap;
  }

  private static String addToUrlEnd(String begin, String string) {
    return begin + (begin.charAt(begin.length() - 1) == '/' ? string : "," + string);
  }

  private static ApplicationInformation checkAppInfo(ApplicationInformation ai) {
    if (ai == null)
      throw new NullPointerException("missing ApplicationInformation");
    return ai;
  }

  private static void checkClass(List<String> names, Class<?> clazz) {
    if (clazz.getResource(clazz.getSimpleName() + ".xml") != null)
      names.add(getClasspathContext(clazz, null));
  }

  private final ApplicationInformation m_applicationInformation;
  private final String[] m_configLocations;
  private Resource[] m_configResources;

  public Configuration(ApplicationInformation appInfo, String[] configLocations) {
    this(null, checkAppInfo(appInfo), configLocations);
  }

  public Configuration(String[] configLocations) {
    this(null, null, configLocations);
  }

  private Configuration(ApplicationContext parent, ApplicationInformation appInfo, String[] configLocations) {
    super(parent);
    LOGGER.info("Initializing " + getClass().getSimpleName() + " for application " + appInfo + ", context containers:");
    for (String cl : configLocations)
      LOGGER.info("  " + cl);
    m_applicationInformation = appInfo;
    m_configLocations = configLocations;
    getConfigResources(); // Initialize config resources
    refresh();
    // Ldap listens to configuration events as well as broadcast messages
    for (Resource resource : getConfigResources()) {
    	    	
    	if (resource instanceof MongoDbResource) {
    		((MongoDbResource) resource).listen(appInfo);
    	}
    }
      
  }

  public ApplicationInformation getApplicationInformation() {
    return m_applicationInformation;
  }

  public Resource[] getConfigResources() {
    if (m_configResources == null) {
      String[] configLocations = m_configLocations;
      List<Resource> resources = new ArrayList<Resource>();
      for (String configLocation : configLocations)
        try {
          for (Resource resource : getResources(configLocation))
            resources.add(resource);
        }
        catch (IOException e) {
          LOGGER.error("Failed to load " + configLocation + ": " + e);
        }
      m_configResources = resources.toArray(new Resource[] {});
    }
    return m_configResources;
  }

  @Override
  public void refresh() throws BeansException, IllegalStateException {
    try {
      super.refresh();
    }
    finally {
    }
  }

  @Override
  protected Resource getResourceByPath(String path) {
    if (m_configResources != null)
      for (Resource r : m_configResources)
        try {
          URL url = r.getURL();
          if (url != null && path.equals(url.toExternalForm()))
            return r;
        }
        catch (IOException e) { // NOPMD
          // Ignore
        }

    try {
      if (path.startsWith("http:") || path.startsWith("https:"))
        return new UrlResource(path);

      if (path.startsWith("mongo:") || path.startsWith("mongos:"))
          return new MongoDbResource(this, path);

      if (path.startsWith("classpath:"))
        return new ClassPathResource(path, Configuration.class);

      return new FileSystemResource(path);
    }
    catch (Throwable t) {
      LOGGER.error( "Failed to load " + path + ": " + t);
      throw CommonUtils.runtimeException(t);
    }
  }

  @Override
  protected void loadBeanDefinitions(UpdateableListableBeanFactory beanFactory) throws IOException, BeansException {
    AbstractBeanDefinitionReader beanDefinitionReader = new XmlBeanDefinitionReader(beanFactory);
    for (Resource resource : getConfigResources()) {
      if (resource instanceof MongoDbResource) {
	        ((MongoDbResource) resource).loadBeanDefinitions(beanFactory);
	  }
      else {
        beanDefinitionReader.loadBeanDefinitions(resource);
      }
    }
  }

  @Override
  protected void onRefresh() throws BeansException {
    super.onRefresh();
    ApplicationInformation ai = getApplicationInformation();
    if (ai != null)
      ai.onRefresh(this);
  }
}