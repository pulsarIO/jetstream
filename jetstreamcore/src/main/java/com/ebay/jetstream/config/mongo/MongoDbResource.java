/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.io.AbstractResource;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.common.NameableThreadFactory;
import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.BeanChangeInformation;
import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.ConfigUtils;
import com.ebay.jetstream.config.Configuration;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.config.ContextBeanChangingEvent;
import com.ebay.jetstream.config.MongoLocator;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.messaging.MessageService;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.ebay.jetstream.util.CommonUtils;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;
/**
 * In order to successfully load from the Mongo Resource application needs to set following 
 * 
 *  	MONGO_HOME environment variable
 *     	Sample MONGO_HOME = slookupmdbd2.vip.qa.ebay.com:27017/jetstream;slookupmdbd1.vip.qa.ebay.com:27017/jetstream;slookupmdbd3.vip.qa.ebay.com:27017/jetstream
 *
 *
 */
@ManagedResource(objectName = "config/mongo/MongoDbResourceV2")
public class MongoDbResource extends AbstractResource implements ApplicationListener, XSerializable, BeanChangeAware {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbResource.class.getPackage().getName());
	private static final String BEAN_NAME = "Config From Mongo";
	private Configuration m_configuration;
	private URL m_url;

	private MongoDataStoreReader m_beanReader; // there is a cycle reference
												// back to mongoresource from
												// reader, need to fix this
	
	private static final String s_topic = "Rtbdpod.local/notification";
	private String mongo_url;
	private final ExecutorService m_executorService = Executors.newCachedThreadPool(new NameableThreadFactory("Jetstream-MongoDbResource"));
	private MongoConfiguration mongoConfiguration;
	private MongoConfigMgr mongoConfigMgr;
	private AtomicBoolean m_registeredMessageServiceListener = new AtomicBoolean(false);
	
	@Hidden
	public boolean getRegisteredMessageServiceListener() {
		return m_registeredMessageServiceListener.get();
	}

	public void setRegisteredMessageServiceListener(
			boolean m_registeredMessageServiceListener) {
		this.m_registeredMessageServiceListener.set(m_registeredMessageServiceListener);
	}

	private boolean doNothing = false;
	
	public BeanFactoryUpdater updater = new BeanFactoryUpdater();
	
	private ConfigFromMongo configFromMongo = new ConfigFromMongo();
	
	public MongoDbResource(Configuration configuration, String path) throws ConfigException {
		m_configuration = configuration;
		// parse & set params scope, application, version, etc
		if(path == null || path.contains("null")) {
			mongo_url = MongoLocator.getMongoLocation();
		} else {
			mongo_url = path;	
		}
		
		if(mongo_url == null) {
			doNothing = true;
			LOGGER.info( "Mongo resource is not set for the application");
		} else {
			parseMongoUrl();

			m_configuration.addApplicationListener(this);

			try {
				mongoConfigMgr = new MongoConfigMgr(mongoConfiguration);
			} catch(Exception e) {
				LOGGER.info( "Error connecting to mongo host and exception is : " +e.getMessage());
			}
			
			Management.removeBeanOrFolder(configFromMongo.getBeanName(), this);
			Management.addBean(configFromMongo.getBeanName(), this);
			
			configFromMongo.setMongoURL(mongoConfiguration.toString());
		}
	}
	
	@Hidden
	public String getBeanNameToDisplay() {
		return BEAN_NAME;
	}
	
	public ConfigFromMongo getConfigFromMongo() {
		return configFromMongo;
	}

	private int getBeanCount() {
		return m_beanReader.getBeanInformation().keySet().size();
	}

	private MongoConfiguration getMongoConfiguration() {

		return mongoConfiguration;
	}

	@Hidden
	public MongoConfigMgr getMongoConfigMgr() {
		return mongoConfigMgr;
	}

	// mongo://server:port//db
	/**
	 * sm-ip2uid01.vip.qa.ebay.com:27017/iptouid;sm-ip2uid02.vip.qa.ebay.com:
	 * 27017/iptouid
	 * slookupmdbd2.vip.qa.ebay.com:27017/unitdb0;slookupmdbd1.vip.
	 * qa.ebay.com:27017/unitdb0;slookupmdbd3.vip.qa.ebay.com:27017/unitdb0
	 * slookupmdbd2
	 * .vip.qa.ebay.com:27017/jetstream;slookupmdbd1.vip.qa.ebay.com:
	 * 27017/jetstream;slookupmdbd3.vip.qa.ebay.com:27017/jetstream
	 * //host:port/db;//host:port/db
	 */
	private void parseMongoUrl() {
		if (mongoConfiguration == null) {
			String db = null;
			String port = null;
			List<String> hosts = new ArrayList<String>();
			try {
				String[] urls = mongo_url.split(";");
				int i=0;
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
					
					if(db == null || port == null || hosts.isEmpty() || (port != null && port.isEmpty())) {
						LOGGER.error( "\n\n Fatal Error : Error in MONGO_HOME(" + urls[i] +") Specification - parsed values of db [" +db + "] , port [" +port + "] , hosts [" + hosts + "]");
					}
					
					i++;
				}
			}
			catch(Exception e) {
				LOGGER.error( "Fatal Error : MONGO_HOME value parsing errors and exception is : " +e.getMessage());
			}
			
			

			mongoConfiguration = new MongoConfiguration();
			mongoConfiguration.setDb(db);
			mongoConfiguration.setHosts(hosts);
			mongoConfiguration.setPort(port);
			// mongoConfiguration.setUser(user);
			// mongoConfiguration.setPw(pw);
		}
	}

	private Map<String, BeanInformation> getBeanInformation() {
		return m_beanReader.getBeanInformation();
	}

	@Hidden
	public Configuration getConfiguration() {
		return m_configuration;
	}

	/**
	 * 
	 * @return String
	 */
	@Hidden
	public String getDescription() {
		return "\n URL: " + mongo_url;
	}

	@Override
	@Hidden
	public File getFile() throws IOException {
		return super.getFile();
	}

	@Override
	@Hidden
	public String getFilename() throws IllegalStateException {
		return super.getFilename();
	}

	/* UnSupported Operation Exception */
	@Hidden
	public InputStream getInputStream() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Hidden
	public MongoDataStoreReader getMongoDataStoreReader() {
		return m_beanReader;
	}
	@Hidden
	public List<String> getScopeList() {
		return m_beanReader.getScopeList();
	}
	@Hidden
	public Map<String, String> getScopeMap() {
		return m_beanReader.getScopeMap();
	}

	@Override
	@Hidden
	public URI getURI() throws IOException {
		return super.getURI();
	}

	/**
	 * @return URL
	 */
	@Override
	@Hidden
	public URL getURL() {
		return m_url;
	}

	public void listen(ApplicationInformation appInfo) {
		if(!doNothing) {
			MessageService ms = MessageService.getInstance();
			
			int count = 10;
			
			// SRM 02/12/2013 - adding following code to wait for message service to come up. Zookeeper transport takes a while to 
			// come up. We need to wait for 30 secs for it to come up before we register lister else we will not be able to register
			// listener.
			
			while (count-- > 0) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					
				}
				if (ms.isInitialized())
					break;
			}
			
			if (ms.isInitialized()) {
				try {
					String configChangeEventTopic = ConfigUtils.getPropOrEnv("CONFIGNOTIFICATIONTOPIC");
					if (configChangeEventTopic == null || configChangeEventTopic.equals(""))
						configChangeEventTopic = s_topic;

					ms.subscribe(new JetstreamTopic(configChangeEventTopic), new MongoConfigChangeListener(m_configuration, appInfo));

					if (LOGGER.isInfoEnabled()) {
						LOGGER.info( "Subscribed for Mongo Config Change Information using Message Service");
					}
					
					setRegisteredMessageServiceListener(true);
					
				} catch (Exception e) {
					throw CommonUtils.runtimeException(e);
				}
			}
			else
				LOGGER.error( "Message Service not initialized - unable to register listener");
			
		}
	}

	/**
	 * 
	 * @param beanFactory
	 * @return
	 */
	public int loadBeanDefinitions(DefaultListableBeanFactory beanFactory) {
		if(doNothing) {
			return -1;
		}
		if (LOGGER.isInfoEnabled()) {
			LOGGER.info( getDescription());
		}

		m_beanReader = new MongoDataStoreReader(beanFactory);
		int numberOfBeans =  m_beanReader.loadBeanDefinitions(this);
				
		return numberOfBeans;

	}
	
	public void addToManagement(MongoConfigDetails mongoConfigDetails, boolean failed) {
		try {
			if(failed) {
				configFromMongo.setFailedMongoConfigDetails(mongoConfigDetails);
			} else {
				configFromMongo.setMongoConfigDetails(mongoConfigDetails);	
			}
			
		} catch(Exception e) {
			LOGGER.warn( "Could not add bean details to management console : " +e.getMessage());
		}
	}
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.context.ApplicationListener#onApplicationEvent(org
	 * .springframework.context.ApplicationEvent)
	 */
	public void onApplicationEvent(ApplicationEvent event) {
		if(!doNothing) {
			if (event instanceof ContextClosedEvent) {
				m_executorService.shutdown();

				try {
					m_executorService.awaitTermination(5, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					String error = CommonUtils.redirectPrintStackTraceToString(e);

					if (LOGGER.isErrorEnabled())
						LOGGER.error(
								"Waiting for shutdown, got InterruptedException: "
										+ error);
				}

			} else {
				BeanFactoryUpdater updater = new BeanFactoryUpdater();
				updater.setApplicationEvent(event);

				// Submit to the Executor Service to execute this task.
				m_executorService.submit(updater);

			}
		}
	}

	public void setURL(URL url) {
		m_url = url;
	}

	private class BeanFactoryUpdater implements Runnable {
		ApplicationEvent event;

		public void run() {
			Configuration configuration = null;
			int sleepSecs = 1000;
			int timesToTry = 3;
			boolean gotValue = false;
			BeanChangeInformation beanChangeInfo = null;
			List<Exception> listOfExceptions = null;

			if (event.getSource() instanceof Configuration)
				try {
					configuration = (Configuration) event.getSource();
					if (event instanceof ContextBeanChangingEvent) {
						
						beanChangeInfo = ((ContextBeanChangingEvent) event).getBeanChangeInformation();
						
						if(beanChangeInfo.getApplication().equals(m_configuration.getApplicationInformation().getApplicationName()) &&
								beanChangeInfo.getVersionString().equals(m_configuration.getApplicationInformation().getConfigVersion()) ) {
							// ToDo
							// have to implement is updateable logic
							if (m_beanReader.isUpdateable(beanChangeInfo)) {
								if (LOGGER.isInfoEnabled()) {
									LOGGER.info( "isUpdateable Method returned true. Update Possible\n");
								}
						
						        //(String application, String scope, String version, String bean, long masterLdapVersion)
							    //beanChangeInfo = new ConfigChangeMessage("JetstreamSamplesApp", "local", "2.0", "SampleTestBean", 1);

								boolean updateStatus = m_beanReader.updateBean(beanChangeInfo);
								if(updateStatus) {
									configuration.publishEvent(new ContextBeanChangedEvent(configuration, beanChangeInfo.getBeanName()));	
								}
									
							} 
						}
					}
				} catch (Throwable e) {
					LOGGER.error( "  Exception in run method of BeanFactoryUpdater ", e);
				}
			

		}

		public void setApplicationEvent(ApplicationEvent event) {
			this.event = event;
		}

		public void stopDuration() {
			double sleepDuration = new SecureRandom().nextDouble() * 1000;

			try {
				Thread.sleep((long) sleepDuration);
			} catch (InterruptedException e) {
				// NO OP
			}
		}

	}
}
