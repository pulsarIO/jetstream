/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ReflectFieldUtil {

	public static Object getField(Object obj, String fieldName)
			throws Exception {
		Class cls = obj.getClass();
		Field field = cls.getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.get(obj);
	}

	public static Object getField(Class cls, Object obj, String fieldName)
			throws Exception {
		Field field = cls.getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.get(obj);
	}

	public static void setField(Object obj, String fieldName, Object value)
			throws Exception {
		Class cls = obj.getClass();
		Field field = cls.getDeclaredField(fieldName);
		field.setAccessible(true);
		field.set(obj, value);
	}

	public static void setField(Class cls, Object obj, String fieldName,
			Object value) throws Exception {
		Field field = cls.getDeclaredField(fieldName);
		field.setAccessible(true);
		field.set(obj, value);
	}

	public static Object invokeMethod(Class cls, Object obj, String methodName,
			Object[] args) throws Exception {
		Class[] argsClass = new Class[args.length];
		for (int i = 0; i < args.length; i++) {
			argsClass[i] = args[i].getClass();
		}
		@SuppressWarnings("unchecked")
		Method method = cls.getDeclaredMethod(methodName, argsClass);
		method.setAccessible(true);
		return method.invoke(obj, args);
	}

	public static Object invokeMethod2(Class cls, Object obj,
			String methodName, Object[] args) throws Exception {
		Method[] methods = cls.getDeclaredMethods();
		for (Method method : methods) {
			if (method.getName().equals(methodName)) {
				method.setAccessible(true);
				return method.invoke(obj, args);
			}
		}
		return null;
	}
}
