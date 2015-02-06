/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jestream.event.annotation;

public class AnnotationConfiguration {

	private String annotation;
	@SuppressWarnings("rawtypes")
	private Class className;
	private AnnotationListener listener;
	private AnnotationProcessor processor;
	
	public String getAnnotation() {
		return annotation;
	}
	public void setAnnotation(String annotationName) {
		this.annotation = annotationName;
	}
	
	public Class getClassName() {
		return className;
	}
	public void setClassName(Class className) {
		this.className = className;
	}
	public AnnotationListener getListener() {
		return listener;
	}
	public void setListener(AnnotationListener annotationListener) {
		this.listener = annotationListener;
	}
	
	public AnnotationProcessor getProcessor() {
		return processor;
	}
	
	public void setProcessor(AnnotationProcessor annotationProcessor) {
		this.processor = annotationProcessor;
	}
	
	@Override
	public String toString() {
		return "AnnotationConfiguration [annotation=" + annotation
				+ ", className=" + className + ", listener=" + listener
				+ ", processor=" + processor + "]";
	}
	
}
