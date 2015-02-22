/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement;

public class MachineInfo {
    private final String ip;
    private final String host;

    public MachineInfo(String ip, String host) {
        this.ip = ip;
        this.host = host;
    }

    public String getIp() {
        return ip;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "ip:" + getIp() + ", host:" + getHost();
    }
}
