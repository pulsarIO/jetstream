/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement;


import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * The utility class that based on google-gson converting JSON to Java objects and
 * vice-versa
 * 
 * 
 * @author weijin
 * 
 */
public final class JsonUtil {

    private static Gson getGson() {
        return new Gson();
    }

    public static <T> T fromJson(String json, Type type) {
        return getGson().fromJson(json, type);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return getGson().fromJson(json, clazz);
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromJson(String json, TypeToken<T> tt) {
        return (T) getGson().fromJson(json, tt.getType());
    }

    public static <T> String toJson(T t) {
        return getGson().toJson(t);
    }

    /** Cannot instantiate. */
    private JsonUtil() { }
}