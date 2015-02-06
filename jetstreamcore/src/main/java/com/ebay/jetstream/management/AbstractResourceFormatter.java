/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.management.Management.BeanFolder;
import com.ebay.jetstream.xmlser.XMLSerializationManager;
import com.ebay.jetstream.xmlser.XMLSerializer;

/**
 * 
 */
public abstract class AbstractResourceFormatter implements ManagedResourceFormatter {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResourceFormatter.class.getPackage().getName());

	public static final String BEAN_FORMAT_PARAM = "$format";

	private final Stack<String> m_stack = new Stack<String>();
	private final String m_serializeFormat;
	private PrintWriter m_writer;
	private String m_prefix;
	private String m_path;
	private Set<String> m_setFilterFields;
	private Stack<String> m_currentPropRoot;

	protected AbstractResourceFormatter(String serializeFormat) {
		m_serializeFormat = serializeFormat;
	}

	public final void format(String prefix, String path, Set<String> setFilterFields, PrintWriter writer) throws Exception {
		Object object = Management.getBeanOrFolder(path);
		m_prefix = prefix;
		m_path = path;
		m_writer = writer;
		m_setFilterFields = setFilterFields;
		if (setFilterFields != null)
			m_currentPropRoot = new Stack<String>();
		
		beginFormat();
		try {
			if (object instanceof BeanFolder) {
				formatFolder((BeanFolder)object);
			}
			else {
				formatBean(object);
			}
		}
		finally {
			endFormat();
			m_writer = null;
			m_path = null;
			m_prefix = null;
		}
	}

	protected void beginFormat() throws IOException { //NOPMD Do nothing by default
		// Do nothing by default
	}

	protected void endFormat() throws IOException { //NOPMD Do nothing by default
		// Do nothing by default
	}

	protected abstract void formatBean(Object bean) throws Exception;

	protected void formatFolder(BeanFolder folder) throws Exception {
		for (String key : folder.keySet()) {
			formatReference(key);
		}
	}

	protected abstract void formatReference(String key) throws Exception;

	protected final String getPath() {
		return m_path;
	}

	protected final String getPrefix() {
		return m_prefix;
	}

	protected final String getReference(String key) {
		return makePath(getPrefix(), getPath(), key);
	}

	protected XMLSerializer getSerializer() {
		return XMLSerializationManager.getSerializer(m_serializeFormat);
	}

	protected PrintWriter getWriter() throws IOException {
		if (m_writer.checkError())
			throw new IOException("write error in " + m_writer);
		return m_writer;
	}

	protected boolean isFilteredQuery() {
		return m_setFilterFields != null;
	}
	
	protected void pushRoot(String strProp) {
		if (m_currentPropRoot != null) {
			String strParent = !m_currentPropRoot.isEmpty() ? m_currentPropRoot.peek() : null;
			if (strParent != null)
				strProp = strParent + "." + strProp;
			LOGGER.debug( "PUSH: " + strProp); 
			m_currentPropRoot.push(strProp);
		}
	}
	
	protected void popRoot() {
		if (m_currentPropRoot != null) {
			String val = m_currentPropRoot.pop();
			LOGGER.debug( "POP: " + val);
		}
	}
	
	protected boolean isAllowedByFilter(String strField) {
		if (m_setFilterFields == null)
			return true;
		
		if (!m_currentPropRoot.isEmpty()) {
			String val = m_currentPropRoot.peek();
			LOGGER.debug( "Root: " + val); 
		} else {
			LOGGER.debug( "Root: null"); 
		}
		LOGGER.debug( "Raw Field: " + strField);
		
		strField = strField.toLowerCase().replaceFirst("^get|is", "");
		
		LOGGER.debug( "Stripped Field: " + strField); 
		
		if (!m_currentPropRoot.isEmpty()) { 
			strField = m_currentPropRoot.peek() + "." + strField;
		}
		
		LOGGER.debug( "Concat Field: " + strField);
		boolean bAllowed = false;
		for (String strFilterField : m_setFilterFields) {
			LOGGER.debug( "Filter: " + strFilterField);
			if (strFilterField.startsWith(strField)) {
				LOGGER.debug( "  Allowed");
				bAllowed = true;
				break;
			}
			LOGGER.debug( "  Denied"); 
		}
		LOGGER.debug( "--------------------");
		return bAllowed;
	}
	
	
	@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="SBSC_USE_STRINGBUFFER_CONCATENATION", justification="Needs to use String.endsWith()")
	protected String makePath(String... strings) {
		String result = "";
		for (String s : strings)
			if (s != null)
				result += (result.length() == 0 || result.endsWith("/") ? "" : "/") + s;
		return result;
	}

	protected void popElement() {
		m_writer.println("</" + m_stack.pop() + ">");
	}

	protected void pushElement(String element, String attributes) {
		m_stack.add(element);
		m_writer.println("<" + element + (attributes == null ? "" : " " + attributes) + ">");
	}
}
