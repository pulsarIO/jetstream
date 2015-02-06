/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.server;

import javax.servlet.Servlet;


/**
 * @author shmurthy
 *
 */
public class ServletHolder {
	private transient Servlet m_servlet;
	private transient Class<? extends Servlet> m_clazz;
	private transient String m_clazzName;

	public Servlet getServlet() {
		return m_servlet;
	}

	/**
	 * @return
	 */
	public Class<? extends Servlet> getClazz() {
		return m_clazz;
	}

	/**
	 * @return
	 */
	public String getClazzName() {
		return m_clazzName;
	}

	/**
	 * @param m_clazzName
	 */
	public void setClazzName(String m_clazzName) {
		this.m_clazzName = m_clazzName;
	}

	public ServletHolder(Servlet servlet) {
		setServlet(servlet);
	}

	public void setServlet(Servlet servlet) {
		if (servlet == null)
			throw new IllegalArgumentException();

		m_servlet = servlet;
		m_clazz = servlet.getClass();
		m_clazzName = servlet.getClass().getName();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		
		if (obj == this)
			return true;

		if (obj == null)
			return false;

		if (!(obj instanceof ServletHolder))
			return false;

		if (!this.m_clazzName.equals(((ServletHolder) obj).m_clazzName))
			return false;
		else
			return true;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return m_clazzName == null ? System.identityHashCode(this)
				: m_clazzName.hashCode();
	}
}
