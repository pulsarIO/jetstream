/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.management;

import java.beans.PropertyDescriptor;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.util.CommonUtils;
import com.ebay.jetstream.xmlser.IXmlDeserializer;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class BeanController {
	private final String m_prefix;
	private final String m_path;
	private final Object m_bean;

	private String m_format;

	private ManagedResourceFormatter m_formatter;
	private Set<String> m_filterFields;

	public BeanController(String prefix, String path) {
		m_prefix = prefix;
		m_path = path;
		m_bean = Management.getBeanOrFolder(path);
	}

	public Object getBean() {
		return m_bean;
	}

	public String getContentType() {
		return m_formatter == null ? "text/plain" : m_formatter.getContentType();
	}

	public String getFormat() {
		return m_format;
	}

	public String getPath() {
		return m_path;
	}

	public String getPrefix() {
		return m_prefix;
	}

	public Object process(Map<String, String[]> parameters) throws Exception {
		Object result = null;
		if (m_bean instanceof ControlBean) {
			result = ((ControlBean) m_bean).process(parameters);
		}
		else {
			for (Entry<String, String[]> entry : parameters.entrySet()) {
				String name = entry.getKey();
				String values[] = entry.getValue();
				if (values == null || values.length == 0 || CommonUtils.isEmptyTrimmed(values[0])) {
					executeAction(name);
				}
				else {
					setProperty(name, values);
				}
			}
		}
		return result;
	}

	void setRequestedFields(String[] aFields) {
		if (aFields != null && aFields.length > 0) {
			m_filterFields = new HashSet<String>();
			for (String strField : aFields)
				m_filterFields.add(strField.toLowerCase());
		}
	}
	
	public void setFormat(String format) throws Exception {
		m_formatter = Management.getResourceFormatter(m_format = format);
	}

	public void write(PrintWriter writer) throws Exception {
		if (m_formatter == null)
			throw new IllegalArgumentException("no formatter found for " + getFormat());
		m_formatter.format(getPrefix(), getPath(), m_filterFields, writer);
	}

	protected Method checkMethodForAnnotation(Method method, Class<? extends Annotation> annotationClass)
	throws IllegalAccessException {
		if (method == null)
			throw new IllegalArgumentException("No method");
		if (!method.isAnnotationPresent(annotationClass))
			throw new IllegalAccessException("Missing " + annotationClass.getSimpleName() + " annotation");
		return method;
	}

	protected void executeAction(String name) throws Exception {
		checkMethodForAnnotation(m_bean.getClass().getMethod(name), ManagedOperation.class).invoke(m_bean);
	}

	protected void setProperty(String name, String values[]) throws Exception {
		PropertyDescriptor pd = new PropertyDescriptor(name, m_bean.getClass());
		Method setter = checkMethodForAnnotation(pd.getWriteMethod(), ManagedAttribute.class);
		Object value = null;
		if (values.length == 1) {
			value = CommonUtils.getObjectFromString(pd.getPropertyType(), values[0]);
		}
		else {
			IXmlDeserializer xs = XMLSerializationManager.getDeserializer(values[0]);
			if (xs == null)
				throw new IllegalArgumentException(values[0] + " is not a supported write format");
			Map<String, Object> deserialized = xs.deserialize(values[1]);
			// String setter = "set" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
			value = deserialized.values().toArray()[0];
		}
		setter.invoke(m_bean, value);
	}
}
