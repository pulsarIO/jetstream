/*******************************************************************************
 * Copyright 2012-2015 eBay Software Foundation
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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