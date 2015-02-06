/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.application.dataflows;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NamedBean;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.EventSink;
import com.ebay.jetstream.event.EventSource;
import com.ebay.jetstream.event.channel.InboundChannel;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * It will capture the flows of data within one application. It should be put
 * within the JetStream application spring container.
 * 
 * @author weijin
 * 
 */
@ManagedResource(objectName = "Event/DataFlows", description = "data flow")
public class DataFlows extends AbstractNamedBean implements BeanFactoryAware,
		InitializingBean, ApplicationListener, XSerializable {
	private Map<String, Set<String>> graph;
	private DefaultListableBeanFactory beanFactory;

	public Map<String, Set<String>> getGraph() {
		populateGraph();
		return graph;
	}

	public void setGraph(Map<String, Set<String>> graph) {
		this.graph = graph;
	}

	public DataFlows() {
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (DefaultListableBeanFactory) beanFactory;
	}

	public void popuateGraph(String beanName,
			DefaultListableBeanFactory beanFactory, DirectedGraph<String> dg) {
		Object object = beanFactory.getBean(beanName);
		if (object instanceof EventSource) {
			Collection<EventSink> eventSinks = ((EventSource) object)
					.getEventSinks();
			for (EventSink eventSink : eventSinks) {
				if (eventSink != null) {
					boolean isNodeNotExistsed = dg.addNode(eventSink
							.getBeanName());
					dg.addEdge(beanName, eventSink.getBeanName());
					if (isNodeNotExistsed) {
						popuateGraph(eventSink.getBeanName(), beanFactory, dg);
					}
				}
			}
		} else if (object instanceof NamedBean) {
			dg.addNode(beanName);
		} else {
			throw new RuntimeException("should not happen!!");
		}
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextStartedEvent) {
			populateGraph();
		}

	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Management.addBean(getBeanName(), this);
	}
	
	private void populateGraph() {
		try {
			String[] inputBeanNames = beanFactory
					.getBeanNamesForType(InboundChannel.class);
			DirectedGraph<String> dg = new DirectedGraph<String>();
			for (String beanName : inputBeanNames) {
				dg.addNode(beanName);
				popuateGraph(beanName, beanFactory, dg);
			}

			graph = dg.getMap();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
