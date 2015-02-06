/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

import com.ebay.jetstream.util.CommonUtils;

public class ConfigUtils {

  public static class StreamHolder implements FactoryBean {
    private final InputStream m_stream;

    @SuppressWarnings("deprecation")
    // deprecated, but we'll use it for now anyway
    public StreamHolder(String contents) {
      m_stream = new java.io.StringBufferInputStream(contents);
    }

    public Object getObject() throws Exception {
      return m_stream;
    }

    public Class<?> getObjectType() {
      return InputStream.class;
    }

    public boolean isSingleton() {
      return false;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class.getName());
  private static final String PKGS = "java.protocol.handler.pkgs";

  /**
   * Adds a package to the list of URL protocol supporters. Each protocol must be in a separate package with the name of
   * the protocol below the given classes package, and must contain a Handler that extends URLStreamHandler.
   * 
   * @param location
   *            the class whose package is added as a root for URLStreamHandlers.
   * 
   * @return true iff the location was added, else false.
   */
  public static boolean addURLProtocols(Class<?> location) {
    String ln = location.getPackage().getName();
    String p = System.getProperty(PKGS);
    boolean add = p == null || !p.contains(ln);
    if (add) {
      p = (CommonUtils.isEmptyTrimmed(p) ? "" : p + "|") + ln;
      System.setProperty(PKGS, p);
    }
    return add;
  }

  public static Class<?> getClassForName(String name) {
    Class<?> result = null;
    try {
      result = name == null ? null : Class.forName(name);
    }
    catch (ClassNotFoundException e) { // NOPMD
      // Ignored
    }
    return result;
  }

  /**
   * Gets a ConfigDataSource for the given path. It could be from a URL, from a file, from the classpath, directory, or
   * from ldap (eventually). Note that the data source could represent multiple items (e.g. in the case of a folder).
   * 
   * @param path
   *            the path to the datasource.
   * @return the ConfigDataSource
   * @throws ConfigException
   *             in case of any problem getting the data source.
   */
  public static ConfigDataSource getConfigDataSource(String path) throws ConfigException {
    if (new File(path).isDirectory())
      return new ConfigDirectory(path, null);

    return new ConfigFile(path);
  }

  /**
   * Gets an individual ConfigStream (an InputStream and its location string) for the given path. The path reference
   * must be to a single item, not a folder.
   * 
   * @param path
   *            the path to the config stream.
   * @return the ConfigStream
   * @throws ConfigException
   *             in case of any problem, including if path is a folder.
   */
  public static ConfigDataSource.ConfigStream getConfigStream(String path) throws ConfigException {
    ConfigDataSource source = getConfigDataSource(path);
    try {
      if (source.isFolder())
        throw new ConfigException("more than one stream found");
    }
    catch (IOException e) {
      throw new ConfigException("cannot get stream: " + e, e);
    }
    Iterator<ConfigDataSource.ConfigStream> ii = source.iterator();
    return ii.hasNext() ? ii.next() : null;
  }

  /**
   * Expands a string with an initial variable. The form of the string should be "${VARIABLE}remainder", where VARIABLE
   * is either a System property or an environment variable (checked in that order).
   * 
   * @param theString
   * @return the string with any initial property expanded
   */
  public static String getInitialPropertyExpanded(String theString) {
	  Pattern pattern = Pattern.compile("\\$\\{([A-Z_a-z0-9.]+)\\}");
	  Matcher matcher = pattern.matcher(theString);
	  while (matcher.find()) {
	      String strVar = matcher.group(1);
	      String strValue = System.getProperty(strVar);
	      if (strValue == null)
	    	  strValue = System.getenv(strVar);
	      if (strValue == null)
	    	  throw new RuntimeException("string value not available for '" + theString + "'");
	      theString = matcher.replaceFirst(strValue.replaceAll("\\\\", "/"));
	      matcher = pattern.matcher(theString);
	  }
	  return theString;
  }

  /**
   * Gets a property from a java Properties, or a system property or process environment variable value, in that order.
   * If none is found, null is returned.
   * 
   * @param properties
   *            the Properties object
   * @param key
   *            the string name of the property or environment variable.
   * @return the property or environment value, or null if none was found.
   */
  public static String getPropOrEnv(Properties properties, String key) {
    String it = properties.getProperty(key);
    return it == null ? getPropOrEnv(key) : it;
  }

  /**
   * Gets a java system property or process environment variable value, in that order. If none is found, null is
   * returned.
   * 
   * @param key
   *            the string name of the property or environment variable.
   * @return the property or environment value, or null if none was found.
   */
  public static String getPropOrEnv(String key) {
    String it = System.getProperty(key);
    if (it == null)
      it = System.getenv(key);
    return it;
  }

  public static Object getValueForType(Class<?> type, String value) throws ConfigException {
    if (String.class.isAssignableFrom(type))
      return value;
    if (Long.class.isAssignableFrom(type) || long.class.isAssignableFrom(type))
      return Long.decode(value);
    if (Integer.class.isAssignableFrom(type) || int.class.isAssignableFrom(type))
      return Integer.decode(value);
    if (Double.class.isAssignableFrom(type) || double.class.isAssignableFrom(type))
      return Double.valueOf(value);
    if (Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type))
      return Boolean.valueOf(value);
    throw new ConfigException("Cannot convert " + value + " to " + type.getName());
  }

  /**
   * Sets properties for java.util.logging.
   * 
   * @param theProperties
   *            the logging properties.
   */
  public static void setLoggingProperties(Properties theProperties) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ByteArrayInputStream is;
    byte[] buffer;
    theProperties.store(os, "LoggingProperties");
    buffer = os.toByteArray();
    is = new ByteArrayInputStream(buffer);
    LogManager.getLogManager().readConfiguration(is);
  }

  public static void setProperties(Object theTarget, Properties theProperties) throws ConfigException {
    Class<?> clazz = theTarget.getClass();
    Method[] methods = clazz.getMethods();
    for (Object key : theProperties.keySet()) {
      if (!(key instanceof String))
        continue;
      String property = (String) key;
      String setter = "set" + (property.startsWith("is") ? property.substring(2) : property);
      for (int i = 0; i < methods.length; i++) {
        Class<?>[] formals = methods[i].getParameterTypes();
        if (formals.length == 1 && setter.equalsIgnoreCase(methods[i].getName())) {
          Object value = getValueForType(formals[0], theProperties.getProperty(property));
          try {
            // Pretty dumb now - doesn't handle overloads or many type conversions
            methods[i].invoke(theTarget, new Object[] { value });
            i = methods.length;
            setter = null;
          }
          catch (Throwable t) {
            throw new ConfigException("Cannot set property " + property + " to value " + value, t);
          }
        } // if a matching setter
      } // for methods
      if (setter != null)
        LOGGER.warn( "No such property found: " + key + " in " + theTarget);
    } // for property keys
  }

  /**
   * Sets the System properties for those given to the new values, and adds properties that do not exist.
   * 
   * @param theProperties
   *            the set of properties to add and update.
   */
  public static void setSystemProperties(Properties theProperties) {
    for (Map.Entry<Object, Object> entry : theProperties.entrySet())
      System.setProperty(entry.getKey().toString(), entry.getValue().toString());
  }

  /**
   * Sets the System properties for those given to the new values, and adds properties that do not exist. Expands any
   * initial dollar-bracket-name-bracket sequence with its environment variable or pre-existing system property value.
   * 
   * @param theProperties
   *            the set of properties to add and update.
   */
  public static void setSystemPropertiesExpanded(Properties theProperties) {
    for (Map.Entry<Object, Object> entry : theProperties.entrySet())
      System.setProperty(entry.getKey().toString(), getInitialPropertyExpanded(entry.getValue().toString()));
  }

  /**
   * Writes a text file from a list of lines to a file by the given name and path. The path can optionally be prefixed
   * with a ${system.property}.
   * 
   * @param theName
   *            the file name, with optional System property prefix.
   * @param theContents
   *            List of content lines.
   * @throws IOException
   */
  public static void writeTextFile(String theName, List<String> theContents) throws IOException {
    theName = getInitialPropertyExpanded(theName);
    File file = new File(theName);
    if (file.isFile() && !file.delete()) {
        throw new IOException("Failed to delete " + file.getCanonicalFile());
    }
    PrintWriter writer = new PrintWriter(file);
    try {
      for (Iterator<String> ii = theContents.iterator(); ii.hasNext();)
        writer.println(ii.next());
    }
    finally {
      writer.close();
    }
  }
}
