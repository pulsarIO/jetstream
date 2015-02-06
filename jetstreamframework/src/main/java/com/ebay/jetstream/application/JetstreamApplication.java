/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.application;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.config.Configuration;
import com.ebay.jetstream.config.RootConfiguration;
import com.ebay.jetstream.event.support.ShutDownOrchestrator;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.util.CommonUtils;
import com.ebay.jetstream.xmlser.Hidden;

/**
 * @author shmurthy
 * 
 *         A container class for hosting Jetstream Event based applications. This class will provide a main method and
 *         shutdown hook. It will start core services like messaging, monitoring and control, logging etc. It will load
 *         the entry bean specified in configuration. The rest of the wiring will be driven by this entry bean. This
 *         class can serve as a main for applications or as a collocated container. If this container's main is used,
 *         then the following command line arguments must be specified Viz, appname, configfile name and config version.
 *         If it is collocated with another container, these 3 parameters can be programatically set.
 * 
 */

@ManagedResource(objectName = "Application")
public class JetstreamApplication {

  private static class ShutdownHook extends Thread {
    @Override
    public void run() {
      try {
        getInstance().shutdown();
      }
      catch (Throwable t) {
        throw CommonUtils.runtimeException(t);
      }
      System.out.println("Gracefully shutdown"); //KEEPME
    }
  }

  private static Class<? extends JetstreamApplication> s_applicationClass = JetstreamApplication.class;
  private static JetstreamApplication s_application;
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.application");
  private ThreadPoolExecutor m_worker;
  private BlockingQueue<Runnable> m_workQueue;

  protected static Class<? extends JetstreamApplication> getApplicationClass() {
    return s_applicationClass;
  }

  public static Configuration getConfiguration() {
    return RootConfiguration.getConfiguration();
  }

  @Hidden
  public static JetstreamApplication getInstance() {
    if (s_application == null) {
      synchronized (s_applicationClass) {
        if (s_application == null) {
          try {
            s_applicationClass.newInstance();
          }
          catch (Exception e) {
            throw CommonUtils.runtimeException(e);
          }
        }
      }
    }
    return s_application;
  }

  /**
   * Every Jetstream application shares a common main(). It creates the instance of the application, configures command
   * line options, parses the command line based on the options, starts the application based on the resulting
   * configuration, and then runs the application.
   * 
   * @param args
   *          command line arguments
   */
  public static void main(String[] args) throws Exception {
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.Jdk14Logger");
    JetstreamApplication ta = null;
    try {
      ta = getInstance();
      // Allow JetstreamApplication option handling methods to be protected
      final JetstreamApplication optionHandler = ta;
      new CliOptions(new CliOptionHandler() {
        public Options addOptions(Options options) {
          return optionHandler.addOptions(options);
        }

        public void parseOptions(CommandLine line) {
          optionHandler.parseOptions(line);
        }
      }, args);
      
      if (System.getenv("COS") == null)
    	  System.setProperty("COS", "Dev");
      
      ta.init();
    }
    catch (Exception e) {
      LOGGER.error( "Failed to start Application" + e.getLocalizedMessage());		
      System.err.println("Failed to start application: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
    ta.run(); // this is the container's event loop
  }

  protected static void setApplicationClass(Class<? extends JetstreamApplication> applicationClass) {
    s_applicationClass = applicationClass;
  }

  private final Object m_monitor = new Object();
  private final AtomicBoolean m_shutdown = new AtomicBoolean(false);

  private final JetstreamApplicationInformation m_applicationInformation = new JetstreamApplicationInformation(this);

  protected JetstreamApplication() {
    if (s_application != null)
      throw new IllegalStateException(s_application.getClass().getName() + " is already running");
    s_application = this;
    RootConfiguration.applicationClass(m_applicationInformation, getClass());
    Runtime.getRuntime().addShutdownHook(new ShutdownHook());
   
  }

  /**
   * Adds options specific to this application. It may be overridden for custom application specific option
   * configuration.
   * 
   * @param options
   *          the Options to configure with new custom application command line options.
   * 
   * @return the configured options.
   */
  protected Options addOptions(Options options) {
    options.addOption("b", "beans", true, "Beans to start during initialization");
    options.addOption("c", "config", true, "Configuration URL or file path");
    options.addOption("cv", "configversion", true, "Version of configuration");
    options.addOption("n", "name", true, "Name of application");
    options.addOption("p", "port", true, "Monitoring port");
    options.addOption("z", "zone", true, "URL or path of dns zone content");
    options.addOption("nd", "nodns", false, "Not a network application");
    options.addOption("wqz", "workqueuesz", true, "work queue size");
    options.addOption("wt", "workerthreads", true, "worker threads");
    return options;
  }

  public JetstreamApplicationInformation getApplicationInformation() {
    return m_applicationInformation;
  }
  
  /**
 * @param work
 * @return true if success else false
 */
  public boolean submitWork(WorkRequest work) {
	  if (m_workQueue != null)
		  return m_workQueue.offer(work);

	  return false;
  }

  /**
   * Override this method to do initialization in a custom JetstreamApplication.
   * 
   * @throws Exception
   */
  protected void init() throws Exception {
    JetstreamApplicationInformation ai = getApplicationInformation();
    ai.selfLocate();
    
    m_workQueue = new LinkedBlockingQueue<Runnable>(ai.getWorkQeueSz());
    m_worker = new ThreadPoolExecutor(ai.getWorkerThreads(), 3, 30, TimeUnit.SECONDS, m_workQueue, new ThreadPoolExecutor.CallerRunsPolicy());
    m_worker.prestartCoreThread();
    
    Management.addBean(ai.getApplicationName(), this);
    logInfo("Starting services for " + ai);
    String[] configs = ai.getConfigRoots();
    RootConfiguration rc = configs == null ? new RootConfiguration(ai) : new RootConfiguration(ai, configs);
    rc.start();
    String[] sa = ai.getBeans();
    if (sa != null)
      for (String bean : sa)
        rc.getBean(bean);
  }

  protected void logInfo(String message) {
	  LOGGER.info( message);
  }

  protected void logSevereError(String message) {
	  LOGGER.error( message);
  }

  /**
   * Parse the options given on the command line. This method may be overridden for application specific command line
   * option handling.
   * 
   * @param commandLine
   *          the parsed command line.
   */
  protected void parseOptions(CommandLine commandLine) {
    JetstreamApplicationInformation ai = getApplicationInformation();
    if (commandLine.hasOption('b')) {
      ai.setBeans(commandLine.getOptionValues('b'));
    }
    if (commandLine.hasOption('c')) {
      ai.setConfigRoots(commandLine.getOptionValues('c'));
    }
    if (commandLine.hasOption("cv")) {
      ai.setConfigVersion(commandLine.getOptionValue("cv"));
    }
    if (commandLine.hasOption('n')) {
      ai.setApplicationName(commandLine.getOptionValue('n'));
    }
    if (commandLine.hasOption('p')) {
      ai.setManagementPort(Integer.valueOf(commandLine.getOptionValue('p')));
      System.setProperty("jetty_port",commandLine.getOptionValue('p'));
    } else{
    	 System.setProperty("jetty_port" ,  String.valueOf(9999));
    }
    if (commandLine.hasOption('z')) {
      ai.setZone(commandLine.getOptionValue('z'));
    }
    if (commandLine.hasOption("nd")) {
      ai.useDNS(false);
    }
    
    if (commandLine.hasOption("wqz")) {
    	ai.setWorkQueueSz(Integer.valueOf(commandLine.getOptionValue("wqz")));
    }
    
    if (commandLine.hasOption("wt")) {
    	ai.setWorkerThreads(Integer.valueOf(commandLine.getOptionValue("wt")));
    }

  }

  /**
   * Override this method to do work in the Jetstream application.
   */
  protected void run() throws Exception {
    // Do nothing by default
  }

  public void shutdown() throws InterruptedException, Exception {
    try {
      if (ShutDownOrchestrator.getInstance() != null) {
        ShutDownOrchestrator.getInstance().shutDown();
      }
      logInfo("Shutting down " + getApplicationInformation().getApplicationName());
    }
    finally {
      Configuration c = getConfiguration();
      if (c != null)
        c.close();
      synchronized (m_monitor) {
        m_shutdown.set(true);
        m_monitor.notifyAll();
        if (m_worker!=null)
        	m_worker.shutdown();
      }
    }
  }

  /**
   * shutdown shuts down all core services. It must be called for graceful exit, and is called automatically if
   * System.exit() is called.
   */
  // 8853 fix - do not show this link no matter what @ManagedOperation

  protected void waitForShutdown() {
    while (!m_shutdown.get()) {
      System.out.println("waiting for shutdown"); //KEEPME
      synchronized (m_monitor) {
        try {
          m_monitor.wait(10000);
        }
        catch (InterruptedException e) {
          // Ignore
        }
      }
    }
  }
}
