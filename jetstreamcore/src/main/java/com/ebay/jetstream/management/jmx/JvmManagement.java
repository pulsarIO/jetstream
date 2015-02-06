/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management.jmx;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryManagerMXBean;
import java.lang.management.MemoryPoolMXBean;

import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.xmlser.XMLSerializationManager;

/**
 * 
 */
public class JvmManagement {
  public JvmManagement() {
    XMLSerializationManager.registerXSerializable(MemoryManagerMXBean.class);
    XMLSerializationManager.registerXSerializable(MemoryPoolMXBean.class);
    Management.addBean("Jvm/ClassLoading", ManagementFactory.getClassLoadingMXBean());
    Management.addBean("Jvm/Compilation", ManagementFactory.getCompilationMXBean());
    Management.addBean("Jvm/GarbageCollector", ManagementFactory.getGarbageCollectorMXBeans());
    Management.addBean("Jvm/MemoryManager", ManagementFactory.getMemoryManagerMXBeans());
    Management.addBean("Jvm/Memory", ManagementFactory.getMemoryMXBean());
    Management.addBean("Jvm/MemoryPool", ManagementFactory.getMemoryPoolMXBeans());
    Management.addBean("Jvm/OS", ManagementFactory.getOperatingSystemMXBean());
    Management.addBean("Jvm/Runtime", ManagementFactory.getRuntimeMXBean());
    Management.addBean("Jvm/Threads", ManagementFactory.getThreadMXBean());
  }
}
