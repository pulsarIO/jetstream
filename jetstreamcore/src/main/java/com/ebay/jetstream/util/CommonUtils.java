/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;


/**
 * @author jvembunarayanan
 */
public class CommonUtils {

 
  public static <T extends Enum<T>> T findEnumIgnoreCase(Class<T> enumClass, String string, T defValue) {
    for (T value : EnumSet.allOf(enumClass))
      if (value.toString().equalsIgnoreCase(string))
        return value;
    return defValue;
  }

  public static Class<?> getCallingClass(int callerIndex) {
    try {
      return Class.forName(new Throwable().getStackTrace()[callerIndex + 1].getClassName());
    }
    catch (ClassNotFoundException e) {
      throw runtimeException(e);
    }
  }

  @SuppressWarnings( { "unchecked", "cast" })
  public static <T> T getDeepCopy(T object) throws InstantiationException, IllegalAccessException {
    T newObject = object;
    if (object instanceof Map) {
      newObject = (T) object.getClass().newInstance();
      for (Entry<Object, Object> entry : ((Map<Object, Object>) object).entrySet()) {
        ((Map) newObject).put(getDeepCopy(entry.getKey()), getDeepCopy(entry.getValue()));
      }
    }
    else if (object instanceof Collection) {
      newObject = (T) object.getClass().newInstance();
      for (Object item : (Collection<Object>) object) {
        ((Collection<Object>) newObject).add(getDeepCopy(item));
      }
    }
    return newObject;
  }

  public static Object getObjectFromString(Class<?> propertyType, String propertyValue) throws Exception {
    if (propertyType.isPrimitive()) {
      propertyType = typeNameToClass(propertyType.getName());
    }
    // Option A: constructor takes a string
    Exception exception = null;
    try {
      return propertyType.getConstructor(String.class).newInstance(propertyValue);
    }
    catch (Exception e) {
      exception = e;
    }
    // Option B: static valueOf takes a string
    try {
      return propertyType.getMethod("valueOf", String.class).invoke(null, propertyValue);
    }
    catch (NoSuchMethodException e) {
      exception = e;
    }
    throw new IllegalArgumentException(propertyType.getCanonicalName()
        + " does not seem to support direct conversion from String: " + exception);
  }

  public static String getResourceAsString(Class<?> clazz, String name, String newLine) throws IOException {
	 InputStream is = clazz.getResourceAsStream(name);
	 try{
		 return CommonUtils.getStreamAsString(is, newLine);
	 }finally{
		 if(is != null)
			 is.close();
	 }
  }

  public static String getStreamAsString(InputStream is, String newLine) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    StringBuilder text = new StringBuilder();
    for (String nl; (nl = reader.readLine()) != null;) {
      text.append(nl);
      if (newLine != null)
        text.append(newLine);
    }
    return text.toString();
  }

  public static boolean isEmpty(Collection<?> c) {
    return c == null || c.size() == 0;
  }

  public static boolean isEmptyTrimmed(String s) {
    return s == null || s.trim().length() == 0;
  }

  @SuppressWarnings("unchecked")
  public static <T> T loadClass(Class<T> clazz) throws ClassNotFoundException {
    return (T) Class.forName(clazz.getName(), true, clazz.getClassLoader());
  }

  public static String redirectPrintStackTraceToString(Throwable cause) {
    StringWriter stringWriter = new StringWriter();
    if (cause != null)
      cause.printStackTrace(new PrintWriter(stringWriter)); // NOPMD
    return stringWriter.toString();
  }

  public static RuntimeException runtimeException(Throwable cause) {
    // Errors are bad news, throw immediately
    if (cause instanceof Error)
      throw (Error) cause;
    return cause instanceof RuntimeException ? (RuntimeException) cause : new RuntimeException(cause);
  }

  public static Class<?> typeNameToClass(String typeName) {
    typeName = typeName.intern();
    if (typeName == "boolean")
      return Boolean.class;
    if (typeName == "byte")
      return Byte.class;
    if (typeName == "char")
      return Character.class;
    if (typeName == "short")
      return Short.class;
    if (typeName == "int")
      return Integer.class;
    if (typeName == "long")
      return Long.class;
    if (typeName == "float")
      return Float.class;
    if (typeName == "double")
      return Double.class;
    if (typeName == "void")
      return Void.class;
    return null;
  }

}
