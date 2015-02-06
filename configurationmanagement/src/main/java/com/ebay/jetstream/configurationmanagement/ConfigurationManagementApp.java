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

import org.springframework.jmx.export.annotation.ManagedOperation;

import com.ebay.jetstream.application.JetstreamApplication;
import com.ebay.jetstream.application.JetstreamApplicationInformation;
import com.ebay.jetstream.util.CommonUtils;

public class ConfigurationManagementApp extends JetstreamApplication {

    static {
        setApplicationClass(ConfigurationManagementApp.class);
    }

    public ConfigurationManagementApp() {
        JetstreamApplicationInformation ai = getApplicationInformation();
        ai.setConfigVersion("1.0");
        ai.setApplicationName("ConfigurationManagement");
    }

    @ManagedOperation
    public void stopApplication() {
        try {
            getInstance().shutdown();
        } catch (Throwable t) {
            throw CommonUtils.runtimeException(t);
        }
        System.out.println("Gracefully stop the application");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}