/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * @author weifang
 * 
 */
public class JsonUtil {
	public static Map<String, Object> jsonStringToMap(String data) {
		try {
			if (data == null)
				return null;
			ObjectMapper jsonMapper = new ObjectMapper();
			TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>() {
			};
			return jsonMapper.readValue(data, type);
		} catch (JsonMappingException e) {
			return null;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Map<String, Object> jsonStreamToMap(InputStream is) {
		try {
			if (is == null)
				return null;
			ObjectMapper jsonMapper = new ObjectMapper();
			TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>() {
			};
			return jsonMapper.readValue(is, type);
		} catch (JsonMappingException e) {
			return null;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static String mapToJsonString(Map<String, Object> data) {
		try {
			ObjectMapper jsonMapper = new ObjectMapper();
			return jsonMapper.writeValueAsString(data);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void mapToJsonStream(Map<String, Object> data, OutputStream os) {
		try {
			ObjectMapper jsonMapper = new ObjectMapper();
			jsonMapper.writeValue(os, data);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
