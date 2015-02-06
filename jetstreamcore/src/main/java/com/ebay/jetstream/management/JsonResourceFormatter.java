/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.management;

/**
 * @author trobison
 */
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.ClassUtils;

import com.ebay.jetstream.xmlser.XMLSerializationManager;

public class JsonResourceFormatter extends AbstractResourceFormatter {

	private final static ThreadLocal<List<Object>> m_refJson = new ThreadLocal<List<Object>>(); 

	JsonResourceFormatter() {
		super("json");
	}

	@Override
	protected void beginFormat() {
	    List<Object> list = m_refJson.get();
	    if (list == null) {
	        list = new ArrayList<Object>();
	        m_refJson.set(list);
	    }
	    list.clear();
	}

	@Override
	protected void endFormat() throws IOException {
		if (!m_refJson.get().isEmpty()) {
		    ObjectMapper mapper = new ObjectMapper();
			getWriter().print(mapper.writeValueAsString(m_refJson.get()));
			beginFormat();
		}
	}

	private Map<String, Object> format(Object objBean, Map<String, Object> objJson, IdentityHashMap identityHashMap) throws Exception {

		Class<?> clazz = objBean.getClass();
		for (PropertyDescriptor pd : Introspector.getBeanInfo(clazz).getPropertyDescriptors()) {
			Method getter = pd.getReadMethod();
			if (!XMLSerializationManager.isHidden(getter) && isAllowedByFilter(getter.getName())) {
				objJson.put(pd.getDisplayName(), formatProperty(objBean, pd, identityHashMap));
			}
		}

		return objJson;
	}

	@Override
	protected void formatBean(Object objBean) throws Exception {
	    IdentityHashMap identityHashMap = new IdentityHashMap();
		// root object
	    Map<String, Object> json = new HashMap<String, Object>();

		// set desc and type on root
		Class<?> clazz = objBean.getClass();
		ManagedResource mr = clazz.getAnnotation(ManagedResource.class);
		String strHelp = mr == null ? null : mr.description();
		if (strHelp != null)
			json.put("description", strHelp);
		json.put("bean", clazz.getName());

		identityHashMap.put(objBean, Boolean.TRUE);
		
		// add properties and operations, recursively as needed
		format(objBean, json, identityHashMap);

		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(Feature.FAIL_ON_EMPTY_BEANS, false);
		// Output
		getWriter().print(mapper.writeValueAsString(json));
	}

	private Object formatProperty(Object bean, PropertyDescriptor pd, IdentityHashMap identityHashMap) throws Exception {

		Object objProp = null;

		Method getter = pd.getReadMethod();
		getter.setAccessible(true);
		

		Class<?> clazz = pd.getPropertyType();
		boolean bIsArray = clazz.isArray();

		try {
			Object objValue = getter.invoke(bean);
			if (objValue == null)
				objValue = bIsArray ? new Object[] {} : "null";

			// add array contents
			pushRoot(pd.getDisplayName().toLowerCase());
			if (bIsArray) {
				List<Object> aItems = new ArrayList<Object>();
				for (Object obj : (Object[]) objValue) {
				    if (obj != null && identityHashMap.containsKey(obj)) {
				        continue;
				    } else if (obj != null) {
				        identityHashMap.put(obj, Boolean.TRUE);
				    }
					aItems.add(recurse(obj, obj.getClass()) ? format(obj, new HashMap<String, Object>(), identityHashMap) : obj);
				}
				objProp = aItems;
			}
			else {
                if (objValue == null || !identityHashMap.containsKey(objValue)) {
                    if (objValue != null) {
                        identityHashMap.put(objValue, Boolean.TRUE);
                    }
    				// add objects
    				objProp = recurse(objValue, clazz) ? format(objValue, new HashMap<String, Object>(), identityHashMap) : objValue;
                }
			}
			
			popRoot();
		}
		catch (Exception e) {
			// eat and return empty
		}

		return objProp;
	}

	@Override
	protected void formatReference(String key) throws Exception {
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("ref", getReference(key));
		m_refJson.get().add(json);
	}

	@Override
	public String getContentType() {
		// RFC 4627
		return "application/json";

	}

	/** @return true if objValue should be formatted recursively */
	private boolean recurse(Object objValue, Class<?> clazz) {
		return !(objValue instanceof String || ClassUtils.isPrimitiveOrWrapper(clazz))
		&& XMLSerializationManager.isXSerializable(clazz);
	}
}
