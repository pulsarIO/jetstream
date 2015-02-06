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

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import com.ebay.jetstream.configurationmanagement.model.DataFlowsObject;

/**
 * The class will get the data flow object by calling the rest service.
 * 
 * @author weijin
 * 
 */
public class DataFlowsUtls {
    private static final String QUERY_STRING = "Event/DataFlows/DataFlows?$format=json";

    public static DataFlowsObject getDataFlowFromMachine(String machine,
            int port) throws Exception {
        DataValidators.NOT_NULL_OR_EMPTY.validate("machine name", machine);

        URL url = new URL("http://" + machine + ":" + port + "/" + QUERY_STRING);
        URLConnection conn = url.openConnection();
        InputStream input = conn.getInputStream();
        StringBuilder builder = new StringBuilder();
        byte[] bytes = new byte[1024 * 8];
        int actualRead = -1;
        while ((actualRead = input.read(bytes)) != -1) {
            builder.append(new String(bytes, 0, actualRead));
        }

        System.out.println(builder.toString());

        return JsonUtil.fromJson(builder.toString(), DataFlowsObject.class);
    }

    private static boolean isEmptyString(String str) {
        return str == null || str.isEmpty();
    }

    public static DataFlowsObject getDataFlow( String machine,
            int port) throws Exception {
        if (isEmptyString(machine) ) {
            throw new RuntimeException(
                    "poolName and machine can not be both empty.");
        }

        return getDataFlowFromMachine(machine, port);
    }
}
