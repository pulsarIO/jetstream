/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
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