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

import java.util.ArrayList;
import java.util.List;

import com.ebay.jetstream.config.mongo.MongoConfiguration;

public class MongoConfigurationFactory {
    private MongoConfiguration mongoConfiguration;

    private MongoConfiguration getMongoConfiguration(String mongo_url) {
        String db = null;
        String port = null;
        List<String> hosts = new ArrayList<String>();

        String[] urls = mongo_url.split(";");
        for (String url : urls) {
            int index = url.indexOf("mongo://");
            url = url.substring(index + "mongo://".length());

            String[] hostAndDb = url.split("/");
            String hostAndPort = hostAndDb[0];
            db = hostAndDb[1];
            String[] hostPort = hostAndPort.split(":");
            String host = hostPort[0];
            port = hostPort[1];

            hosts.add(host);
        }

        if (db == null || port == null || hosts.isEmpty()) {
            throw new IllegalArgumentException();
        }

        mongoConfiguration = new MongoConfiguration();
        mongoConfiguration.setDb(db);
        mongoConfiguration.setHosts(hosts);
        mongoConfiguration.setPort(port);
        // mongoConfiguration.setUser(user);
        // mongoConfiguration.setPw(pw);

        return mongoConfiguration;
    }

    private String getPropOrEnv(String key) {
        String it = System.getProperty(key);
        if (it == null) {
            it = System.getenv(key);
        }
        return it;
    }

    public synchronized MongoConfiguration getMongoConfiguration() {
        if (mongoConfiguration == null) {
            try {
                getMongoConfiguration(getPropOrEnv("MONGO_HOME"));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        return mongoConfiguration;
    }
}
