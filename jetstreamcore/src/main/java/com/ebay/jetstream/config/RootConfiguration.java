/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.ebay.jetstream.util.CommonUtils;

public class RootConfiguration extends Configuration {
	private static final String JETSTREAM_HOME = "JETSTREAM_HOME";
	private static Configuration s_configuration;
	private static String s_configurationRoot;

	static {
		String root = System.getProperty("com.ebay.jetstream.config");
		if (root == null) {
			root = ConfigUtils.getPropOrEnv(JETSTREAM_HOME);
			if (CommonUtils.isEmptyTrimmed(root)) 
				root = System.getProperty("user.home");

			root = root.trim();
			if (!root.endsWith(File.separator))
				root += File.separator;
			root +=  "JetstreamConf";
		}
		
		// strip double slashes, but dont use regex methods because "\" vs "/" will be tricky
		// to dynamically throw into a regex
		String strDoubles = File.separator + File.separator;
		while (root.indexOf(strDoubles) != -1)
			root = root.replace(strDoubles, File.separator);
		
		RootConfiguration.setConfigurationRoot(root);
	}

  public static ApplicationInformation applicationClass(ApplicationInformation ai, Class<?> clazz) {
    return ai.get("className") == null ? setApplicationInformation(ai, "className", clazz.getCanonicalName()) : ai;
  }

  /**
   * Gets the instance of a config object from the configuration container.
   * 
   * @param what
   *            config object to get
   * @return the config bean instance
   */
  public static Object get(String what) {
    return s_configuration.getBean(what);
  }

  /**
   * Returns the ApplicationContext for the RootConfiguration. It can be used for refreshing the context, etc.
   * 
   * @return this application context.
   */
  public static Configuration getConfiguration() {
    return s_configuration;
  }

  public static String getConfigurationRoot() {
    return s_configurationRoot;
  }

  /**
   * Calculates the list of default Spring context files. This list consists first of classpath context files, then
   * files from the configuration folder. The classpath context files are class resource files ending in .xml, named the
   * same as the "className" class in the ApplicationInformation, then its super class, etc, and then all implemented
   * interfaces. The classpath lookup mechanism is disabled if the class is null or cannot be loaded. The configuration
   * folder is specified by the com.ebay.jetstream.config system property if it exists, and if it does not exist, then the
   * "JetstreamConf" folder under the location contained in the JETSTREAM_HOME system property or environment variable
   * (checked in that order).
   * 
   * @return the array of default context files.
   */
  public static String[] getDefaultContexts(ApplicationInformation ai) {
    List<String> contexts = new ArrayList<String>();
    Class<?> contextClass = ConfigUtils.getClassForName(ai.get("className"));
    if (contextClass != null)
      contexts.addAll(getClasspathContexts(contextClass));
    contexts.addAll(getContexts(getConfigurationRoot()));
    contexts.addAll(getLdapContexts(ai));
    contexts.addAll(getMongoContexts());
    
    return contexts.toArray(new String[contexts.size()]);
  }

  public static void setConfigurationRoot(String theRoot) {
    LOGGER.info( "Configuration root is: " + theRoot);
    s_configurationRoot = theRoot;
  }

  protected static ApplicationInformation setApplicationInformation(ApplicationInformation ai, String key, String value) {
    ai.set(key, value);
    return ai;
  }

  /**
   * Initializes the root configuration with default context relative to the calling (instantiating class).
   * 
   * @param ai
   *            the application information
   * 
   * @see getDefaultContexts
   */
  public RootConfiguration(ApplicationInformation ai) {
    this(ai, getDefaultContexts(applicationClass(ai, CommonUtils.getCallingClass(2))));
  }

  /**
   * @param ai
   *            the application information, which should come from the build.
   * @param contexts
   *            the array of paths to application configuration.
   */
  public RootConfiguration(ApplicationInformation ai, String[] contexts) {
    super(ai, contexts);
    if (s_configuration != this)
      throw new IllegalStateException("found unexpected root configuration: " + s_configuration.getClass().getName());
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification="Okay in this release, will look at fixing later.")
  public void refresh() {
    s_configuration = this; // FIXME
    super.refresh();
  }
}
