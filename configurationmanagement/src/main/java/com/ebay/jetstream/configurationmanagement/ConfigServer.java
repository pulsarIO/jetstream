/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.context.event.ContextStoppedEvent;

import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.util.CommonUtils;

public class ConfigServer implements ApplicationListener<ApplicationEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServer.class.getName());
    private Server s_server = null;

    public int getPort() {
        return s_port;
    }

    public void setPort(int s_port) {
        this.s_port = s_port;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {

        if (event instanceof ContextStartedEvent) {
            this.startStandAlone();
        } else if (event instanceof ContextClosedEvent || event instanceof ContextStoppedEvent) {
            this.stopStandAlone();
        }
    }

    private final AtomicBoolean running = new AtomicBoolean(false);
    private int s_port;
    private static final String WEB_HOME_ENV = "JETSTREAM_HOME";

    private String getBaseUrl() {
        String root = ConfigUtils.getPropOrEnv(WEB_HOME_ENV);
        if (root == null) {
            throw new RuntimeException(WEB_HOME_ENV + " is not specified.");
        }

        return root + "/webapp";
    }

    public void startStandAlone() {
        try {

            WebAppContext context = new WebAppContext();
            String baseUrl = getBaseUrl();
            LOGGER.info("Config server baseUrl: " + baseUrl);
            context.setDescriptor(baseUrl + "/WEB-INF/web.xml");
            context.setResourceBase(baseUrl);
            context.setContextPath("/");
            context.setParentLoaderPriority(true);
            Server s_server = new Server(s_port);
            // Jetty8 can not set thread pool size.
            s_server.setHandler(context);

            LOGGER.info( "Config server started, listening on port " + s_port);
            s_server.start();
            running.set(true);

        } catch (Throwable t) {
            throw CommonUtils.runtimeException(t);
        }
    }

    public void stopStandAlone() {
        if (s_server != null) {
            try {
                s_server.stop();
                s_server = null;
                running.set(false);
            } catch (Exception e) {
            }
        }
    }
}
