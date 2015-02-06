/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;

import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.ClassUtils;

import com.ebay.jetstream.util.CommonUtils;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

/**
 * 
 */
public class HtmlResourceFormatter extends AbstractResourceFormatter {
	private String m_format = "html";

	public HtmlResourceFormatter() {
		super("spring");
	}

	public String getContentType() {
		return "text/html";
	}

	@Override
	protected void beginFormat() throws IOException {
		pushElement("html", null);
		getWriter().println(
				"<H1>Viewing as <B>" + getFormat() + "</B></H1><P/>");
		formatTitleLink(1, makeFormattedPath(getFormat(), getPrefix()),
				"Directory Root");
		formatTitleLink(3, "/visualpipeline", "Visual Data Pipeline");
		if (!CommonUtils.isEmptyTrimmed(getPath())) {
			formatTitleLink(2,
					makeFormattedPath(getFormat(), getPrefix(), getPath()),
					getPath());
		}
	}

	@Override
	protected void endFormat() throws IOException {
		getWriter().println("<P/><H3><I>View as</I></H3><P/>");
		for (String format : Management.getResourceFormatters()) {
			formatHRef(makeFormattedPath(format, getPrefix(), getPath()),
					format);
		}
		popElement();
	}

	protected void formatBean(boolean end, Class<?> bclass, String help)
			throws IOException {
		PrintWriter pw = getWriter();
		if (!CommonUtils.isEmptyTrimmed(help)) {
			pw.print("<I>" + help + "</I>: ");
		}
		pw.println("type " + bclass.getName() + "<P/>");
	}

	@Override
	protected final void formatBean(Object bean) throws Exception {
		Class<?> bclass = bean.getClass();
		ManagedResource mr = bclass.getAnnotation(ManagedResource.class);
		String help = mr == null ? null : mr.description();
		formatBean(false, bclass, help);
		boolean section = false;
		for (PropertyDescriptor pd : Introspector.getBeanInfo(bclass)
				.getPropertyDescriptors()) {
			Method getter = pd.getReadMethod();
			if (!XMLSerializationManager.isHidden(getter)) {
				if (!section) {
					section = true;
					formatSection(false, "Properties");
				}
				formatProperty(bean, pd);
			}
		}
		if (section) {
			formatSection(true, "Properties");
		}
		section = false;
		for (Method method : bean.getClass().getMethods()) {
			ManagedOperation mo = method.getAnnotation(ManagedOperation.class);
			if (mo != null && method.getParameterTypes().length == 0) {
				help = mo.description();
				if (!section) {
					section = true;
					formatSection(false, "Operations");
				}
				formatOperation(method);
			}
			if (section) {
				formatSection(true, "Operations");
			}
		}
	}

	protected void formatHRef(String ref, String text) throws IOException {
		getWriter().println("<A href=\"" + ref + "\">" + text + "</A>");
	}

	protected void formatOperation(Method method) throws IOException {
		PrintWriter pw = getWriter();
		String text = method.getName();
		formatHRef(makePath(getPrefix(), getPath(), "?" + text), text);
		text = method.getAnnotation(ManagedOperation.class).description();
		if (!CommonUtils.isEmptyTrimmed(text)) {
			pw.print(" (" + text + ")");
		}
		pw.println();
	}

	protected void formatProperty(Object bean, PropertyDescriptor pd)
			throws Exception {
		PrintWriter pw = getWriter();
		Method getter = pd.getReadMethod();
		Class<?> pclass = pd.getPropertyType();
		ManagedAttribute attr = getter.getAnnotation(ManagedAttribute.class);
		String text = attr != null ? attr.description() : null;
		if (CommonUtils.isEmptyTrimmed(text)) {
			text = pd.getDisplayName();
		} else {
			text = pd.getDisplayName() + " (" + text + ")";
		}
		pw.print(text + ": " + pclass.getName() + " = ");
		getter.setAccessible(true);
		Object value = getter.invoke(bean);
		Method setter = pd.getWriteMethod();
		attr = setter == null ? null : setter
				.getAnnotation(ManagedAttribute.class);
		boolean isComplex = !(String.class.isAssignableFrom(pclass) || ClassUtils
				.isPrimitiveOrWrapper(pclass));
		if (isComplex) {
			value = StringEscapeUtils.escapeXml(getSerializer()
					.getXMLStringRepresentation(value));
		}
		if (attr == null) {
			if (isComplex) {
				pushElement("code", null);
			}
			pw.println(value);
			if (isComplex) {
				popElement();
			}
		} else {
			pw.println(attr.description());
			pushElement(
					"form",
					"action="
							+ makePath(getPrefix(), getPath(),
									isComplex ? "?form" : "?" + pd.getName())
							+ " method=" + (isComplex ? "POST" : "GET"));
			if (isComplex) {
				pw.print("<TEXTAREA name=" + pd.getName() + " rows=4 cols=32>"
						+ value + "</TEXTAREA>");
			} else {
				pw.print("<input type=text name=" + pd.getName() + " value=\""
						+ value + "\"/>");
			}
			pw.println("<input type=submit Value=\"Go\"/>");
			popElement();
		}
		pw.println("<P/>");
	}

	@Override
	protected void formatReference(String key) throws IOException {
		formatHRef(getReference(key) + "?" + BEAN_FORMAT_PARAM + "="
				+ getFormat(), key);
		getWriter().println("<P/>");
	}

	protected void formatSection(boolean end, String type) throws IOException {
		if (!end) {
			getWriter().println("<H3>" + type + "</H3><P/>");
		}
	}

	protected void formatTitleLink(int level, String link, String text)
			throws IOException {
		pushElement("h" + level, null);
		formatHRef(link, text);
		popElement();
	}

	protected String getFormat() {
		return m_format;
	}

	protected String makeFormattedPath(String format, String... strings) {
		String s = makePath(strings) + "?" + BEAN_FORMAT_PARAM + "=" + format;
		return s;
	}

	protected void setFormat(String format) {
		m_format = format;
	}
}
