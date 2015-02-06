/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.dynamicconfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.MongoLocator;
import com.ebay.jetstream.config.RootConfiguration;
import com.ebay.jetstream.config.mongo.JetStreamBeanConfigurationDo;
import com.ebay.jetstream.config.mongo.MongoConfigMgr;
import com.ebay.jetstream.config.mongo.MongoConfiguration;

/**
 * Tool to upload spring bean config to mongo database.
 * 
 * Run it as Java Application.
 * 
 * It requires the following to execute successfully
 * 
 * 1. MONGO_HOME environment variable Sample MONGO_HOME =
 * slookupmdbd2.vip.qa.ebay
 * .com:27017/jetstream;slookupmdbd1.vip.qa.ebay.com:27017
 * /jetstream;slookupmdbd3.vip.qa.ebay.com:27017/jetstream
 * 
 * 2. Options you can pass as arguments to application
 * 
 * -app=JetstreamSamplesApp -beandefxml=C:\TestBeanFile.xml
 * -beanid=SampleTestBean -scope=local -user=naga -version=1.2
 * 
 * OR
 * 
 * -app=JetstreamSamplesApp -beandefxml=C:\TestBeanFile.xml,C:\TestBeanFile2.xml
 * -beanid=SampleTestBean,SampleTestBean -scope=local -user=naga -version=1.2
 * 
 * OR
 * 
 * -app=JetstreamSamplesApp,TestApp
 * -beandefxml=C:\TestBeanFile.xml,C:\TestBeanFile2.xml
 * -beanid=SampleTestBean,SampleTestBean -scope=local,global -user=naga
 * -version=1.2,1.3
 * 
 * 
 * app is Application Name beandefxml = xml file containing spring bean
 * definition. Required to Use different file for different beans so that you
 * can also pass the bean name. beanid = name of the bean. Keep it in sync with
 * the name in beandefxml. scope = local or global or dc specific version =
 * Application Version
 */

public class ConfigUploaderToMongo {
	@SuppressWarnings("restriction")
	private MongoConfiguration mongoConfiguration = null;
	@SuppressWarnings("restriction")
	private MongoConfigMgr mongoConfigMgr = null;

	private final Options options = new Options();
	private final CommandLineParser parser = new GnuParser();

	private boolean invalidOption = true;
	private boolean getBeanName = false;

	private String application;
	private String scope;
	private String version;
	private String user;
	private String beanId;
	private String beanDefXml;
	private boolean publish = true;
	private boolean display = false;
	private boolean delete = false;
	private boolean reupload = true;

	private String[] beanDefXmls;
	private String[] beanIds;

	private String beanVersion;
	
	private boolean multipleBeanDefXmls = false;
	private boolean multipleBeanIds = false;

	PublishConfigMessage publisher = null;
	
	String beans_beginning_tag = "\n" + "<beans xmlns=\"http://www.springframework.org/schema/beans\"" + "\n" +
		"xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" + "\n" +
		"xsi:schemaLocation=\"http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd\"" + "\n" +
		"default-lazy-init=\"false\">" + "\n\n";
	
	String beans_end_tag = "\n\n" + "</beans>";
	
	
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUploaderToMongo.class.getPackage().getName());
	
	
	@SuppressWarnings("restriction")
	public ConfigUploaderToMongo(String[] args) throws ConfigException {

		initCommandLineParameters();
		if (parseOptions(args)) {

			ApplicationInformation ai = new ApplicationInformation(
					"ConfigChange", "0.0.0.0");

			List<String> configs = RootConfiguration
					.getContexts(RootConfiguration.getConfigurationRoot());
			
			String[] configArray = new String[configs.size()];
			new RootConfiguration(ai, configs.toArray(configArray));
			
			getMongoConfiguration();
			
			publisher = new PublishConfigMessage();

		}
		
		if(args != null) {
			LOGGER.info( "Config Upload user input is : " +Arrays.toString(args));	
		}
	}
	
	public boolean isDisplay() {
		return display;
	}
	
	public boolean isDelete() {
		return delete;
	}
	
	public void display() throws Exception  {
		if(application != null && version != null) {
			List<JetStreamBeanConfigurationDo> currentConfigs = new ArrayList();
			if(getBeanName) {
				currentConfigs = mongoConfigMgr.getJetStreamConfiguration(application, version);	
			} else {
				if(multipleBeanIds) {
					for (int i = 0; i < beanIds.length; i++) {
						beanId = beanIds[i];
						if(beanId != null) {
							currentConfigs.addAll(mongoConfigMgr.getJetStreamConfiguration(application, version, beanId));	
						}
					}
				} else {
					if(beanId != null) {
						currentConfigs.addAll(mongoConfigMgr.getJetStreamConfiguration(application, version, beanId));
					}
				}
			}
				
			
			if(currentConfigs == null || currentConfigs.isEmpty()) {
				LOGGER.info( "\n\n There is no config for application : " +application + ", version : " +version);
			}
			
			LOGGER.info( "\n\n Displaying config for application : " +application + ", version : " +version);
			for(JetStreamBeanConfigurationDo currentConfig : currentConfigs) {
				LOGGER.info( "\n\n Config details  : " +currentConfig.toString());
			}
		}
	}
	
	public void delete() throws Exception  {
		if(application != null && version != null && beanId != null) {
			LOGGER.info( "\n\n Deleting config for application : " +application + ", version : " +version + ", beanId : " +beanId);
			
			List<JetStreamBeanConfigurationDo> configs = mongoConfigMgr.getJetStreamConfiguration(application, version, beanId);
			
			if(configs.size() > 1) {
				LOGGER.info( " found more than 1 config for application : " +application + ", version : " +version + ", beanId : " +beanId + " -- size is  : " +configs.size());
			}
			
			JetStreamBeanConfigurationDo config = configs.get(0);
			LOGGER.info( "Config being deleted is " +config.toString());
			
			boolean result = mongoConfigMgr.removeJetStreamConfiguration(application, version, beanId, config.getBeanVersion(), config.getScope());
			
				
			if(result) {
				LOGGER.info( "Deleted config for application" );
			}
			
			uploadPreviousConfig();
		}
	}
	
	private void uploadPreviousConfig() throws Exception {
		if(reupload) {
			List<JetStreamBeanConfigurationDo> configs = mongoConfigMgr.getJetStreamConfiguration(application, version, beanId);
			if(configs.size() > 1) {
				LOGGER.info( " found more than 1 config for application : " +application + ", version : " +version + ", beanId : " +beanId + " -- size is  : " +configs.size());
			}
			
			JetStreamBeanConfigurationDo config = configs.get(0);
			LOGGER.info( "Config being re-uploaded is " +config.toString());
			
			scope = config.getScope();
			user = "AutoUploadAfterDelete";
			
			uploadSingle(beanId, config.getBeanDefinition());
			publishSingle(beanId);
			
			LOGGER.info( "Config re-uploaded is DONE " );	
		}
	}

	public void upload() throws Exception {
		if(getBeanName) {
			// user did not pass bean id, so get all the beans from xml file(s) and upload it separately
			//if(beanDefXmls != null && beanDefXmls.length > 0) {
			if(multipleBeanDefXmls) {
				for (int i = 0; i < beanDefXmls.length; i++) {
					beanDefXml = beanDefXmls[i];
					uploadAndPublishAllBeanIdsFromFile();
				}
			} else {
				uploadAndPublishAllBeanIdsFromFile();	
			}
		} else {
			// user passed bean id, so just work on those id's
			
			if(multipleBeanIds) {
				for (int i = 0; i < beanIds.length; i++) {
					beanId = beanIds[i];
					uploadAndPublishBeanId();
				}
			} else {
				uploadAndPublishBeanId();
			}
		}
		
	}

	private void uploadAndPublishBeanId() throws Exception {
		// single bean id - have to search in multiple files possibly
		if(multipleBeanDefXmls) {
			
			for (int i = 0; i < beanDefXmls.length; i++) {
				beanDefXml = beanDefXmls[i];
				if(uploadAndPublishBeanIdFromFile(beanId)) {
					break; // found the bean definition, do not need to look in other xml files
				} 
				// else continue looking for beanid in other xml files
			}
			
		} else {
			uploadAndPublishBeanIdFromFile(beanId);	
		}
	}
	
	private void uploadAndPublishAllBeanIdsFromFile() throws Exception {
		File beanFile = new File(beanDefXml);
		List<String> beanIdsList = XmlUtil.getBeanIds(beanFile);
		if(beanIdsList != null && !beanIdsList.isEmpty()) {
			LOGGER.info( "Number of Bean Ids are [" + beanIdsList.size() + "]" + " from bean definition file : " +beanDefXml);
			LOGGER.info( "Bean Ids are : " +beanIdsList.toString());
			
			//beanIds = (String[])beanIdsList.toArray();
			for(String beanId : beanIdsList) {
				if(beanId != null && beanId.trim().length() > 0) {
					String beanDefinition = XmlUtil.getBeanDefinition(beanId, beanFile);
					beanDefinition = appendBeanTags(beanDefinition);
					
					if(beanDefinition != null && beanDefinition.trim().length() > 0) {
						LOGGER.info( "Uploading following spring bean definition for " +beanId + " from bean definition file : " +beanDefXml);
						LOGGER.info( "\n" + beanDefinition);
						//CalEventHelper.
						
						uploadSingle(beanId, beanDefinition);
						publishSingle(beanId);
					} else {
						LOGGER.info( "Did NOT find bean definition for " +beanId + " from bean definition file : " +beanDefXml);
					}
				} else {
					LOGGER.info( "Did NOT do upload because of Bean Id being [" +beanId + "]");
				}
			}
		}	
	}
	
	private boolean uploadAndPublishBeanIdFromFile(String beanId) throws Exception {
		File beanFile = new File(beanDefXml);
		if(beanId != null && beanId.trim().length() > 0) {
			String beanDefinition = XmlUtil.getBeanDefinition(beanId, beanFile);
			if(beanDefinition == null) {
				LOGGER.info( "Did NOT find bean definition for " +beanId + " from bean definition file : " +beanDefXml);
				return false;
			}
			beanDefinition = appendBeanTags(beanDefinition);
			
			if(beanDefinition != null && beanDefinition.trim().length() > 0) {
				LOGGER.info( "Uploading bean definition for " +beanId + " from bean definition file : " +beanDefXml);
//				LOGGER.info( "Uploading following spring bean definition for " +beanId + " from bean definition file : " +beanDefXml);
//				LOGGER.info( "\n" + beanDefinition);
				
				uploadSingle(beanId, beanDefinition);
				publishSingle(beanId);
				
				return true; // indicates success
			} 
		} else {
			LOGGER.info( "Did NOT do upload because of Bean Id being [" +beanId + "]");
		}
		
		return false;
	}
	
	// can optimize it later to notify on multiple bean updates instead of one.
	@SuppressWarnings("restriction")
	public void uploadSingle(String beanId, String beanDefinition) throws Exception {

		String beanVer = "0";
		JetStreamBeanConfigurationDo currentConfig = mongoConfigMgr
				.getJetStreamConfiguration(application, version, beanId, scope);
		if (currentConfig != null) {
			int currentVersion = Integer
					.valueOf(currentConfig.getBeanVersion());
			beanVer = String.valueOf(currentVersion + 1);
		}

		beanVersion = beanVer; // to publish the event

		JetStreamBeanConfigurationDo configDo = new JetStreamBeanConfigurationDo();

		configDo.setAppName(application);
		configDo.setVersion(version);
		configDo.setScope(scope);

		configDo.setBeanName(beanId);
		configDo.setBeanDefinition(beanDefinition);
		configDo.setBeanVersion(beanVersion);

		configDo.setCreatedBy(currentConfig != null ? currentConfig
				.getCreatedBy() : user);
		Date currentDate = new Date();
		configDo.setCreationDate(currentConfig != null ? currentConfig
				.getCreationDate() : currentDate.getTime());
		configDo.setModifiedBy(user);
		configDo.setModifiedDate(currentDate.getTime());

		uploadConfig(configDo);
	}
	
	private String appendBeanTags(String beanDefinition) {
		int index = beanDefinition.indexOf("<bean");
		StringBuffer beanDef = new StringBuffer(beanDefinition);
		beanDef.insert(index, beans_beginning_tag);
		beanDef.append(beans_end_tag);
		
		return beanDef.toString();
	}

	private void publish() {
		if(publish) {
			publisher.publish(application, scope, version, beanId, beanVersion);
			LOGGER.info( "Publish SUCCESSFUL for bean : " + beanId);			
		}
	}
	
	private void publishSingle(String beanId) {
		if(publish) {
			LOGGER.info( "Attempting to Publish for bean : " + beanId);
			publisher.publish(application, scope, version, beanId, beanVersion);			
		}
	}

	private void uploadConfig(JetStreamBeanConfigurationDo configDo) {
		
		mongoConfigMgr.uploadJetStreamConfiguration(configDo);

		LOGGER.info( "UPLOAD to Mongo SUCCESSFUL for bean : "
				+ beanId + " and bean details are : " +configDo.toString());
	}

	private void getMongoConfiguration(String mongo_url) {
		// it should not be null
		LOGGER.info( " Mongo URL is : " +mongo_url);
		if(mongo_url == null || mongo_url.length() < 1) {
			logErrorAndExit("Fatal Error : MONGO_HOME is not set and it is required to run the application. \n Fatal Error : MONGO_HOME sample example value is slookupmdbd2.vip.qa.ebay.com:27017/jetstream", null);
		}
		
		if (mongoConfiguration == null) {
			String db = null;
			String port = null;
			List<String> hosts = new ArrayList<String>();
			
			try {
				
				
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
			} catch(Exception e) {
				logErrorAndExit("Fatal Error : MONGO_HOME value parsing errors for url : " +mongo_url + " and exception is : " +e.getMessage(), e);
			}
			
			if(db == null || port == null || hosts.isEmpty()) {
				logErrorAndExit("Fatal Error : check the value of MONGO_HOME and parsed values of db [" +db + "] , port [" +port + "] , hosts [" + hosts + "]", null);
			}
			
			mongoConfiguration = new MongoConfiguration();
			mongoConfiguration.setDb(db);
			mongoConfiguration.setHosts(hosts);
			mongoConfiguration.setPort(port);
			// mongoConfiguration.setUser(user);
			// mongoConfiguration.setPw(pw);
		}
		
		try {
			mongoConfigMgr = new MongoConfigMgr(mongoConfiguration);
		} catch(Exception e) {
			logErrorAndExit("Fatal Error : Could not establish connection to mongo host " +e.getMessage(), e);
		}
	}
	
		
	
	private void logErrorAndExit(String msg, Exception e) {
		if(e != null) {
			LOGGER.info( msg, e);
		} else {
			LOGGER.info( msg);
		}
		System.exit(1);
	}

	private MongoConfiguration getMongoConfiguration() {
		String mongoUrl = null;
		if (mongoConfiguration == null) {
			mongoUrl = getPropOrEnv("MONGO_HOME");
			LOGGER.info( " Mongo URL from ENV variable is : " +mongoUrl);
			
			if( mongoUrl == null || mongoUrl.length() < 1 ) {
				mongoUrl = MongoLocator.getMongoLocation();
				LOGGER.info( " Mongo URL from DNS is : " +mongoUrl);
			}
			
			getMongoConfiguration(mongoUrl);
		}

		return mongoConfiguration;
	}

	private String getPropOrEnv(String key) {
		String it = System.getProperty(key);
		if (it == null)
			it = System.getenv(key);
		return it;
	}

	// cmd line arguments
	private void initCommandLineParameters() {
		/* Specify options */
		options.addOption("h", false, "help");
		options.addOption("app", true, "Application Name");
		options.addOption("version", true, "Version");
		options.addOption("scope", true, "Scope");
		options.addOption("user", true, "User Name");
		options.addOption("beanid", true, "Bean Name");
		options.addOption("beandefxml", true, "Bean Def Xml");
		options.addOption("publish", true, "Publish to Apps");
		options.addOption("display", true, "Display Config of App");
		options.addOption("delete", true, "Delete Config of App");
		options.addOption("beanversion", true, "Bean Version");
		options.addOption("reupload", true, "Auto upload after Delete");
	}

	private boolean parseOptions(String[] args) {
		CommandLine line = null;

		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			LOGGER.info( "Exception happened parsning cmd line options");
			throw new RuntimeException(e);
		}

		if (line != null) {
			/* Check for HELP */
			if (line.hasOption("-h")) {
				printUsage();
				return !invalidOption;
			}

			if (line.hasOption("-display")) {
				if (getOtherOptionValuesForDisplay(line)) {
					invalidOption = false;
				}
			} else if (line.hasOption("-delete")) {
				if (getOtherOptionValuesForDelete(line)) {
					invalidOption = false;
				}
			} else {
				if (getOtherOptionValues(line)) {
					invalidOption = false;
				}	
			}
			
		}

		if (invalidOption) {
			printUsage();
		}

		return !invalidOption;
	}

	private void printUsage() {
		/* Help Information */
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("ConfigChangeOptions", options);
	}

	private boolean getOtherOptionValues(CommandLine line) {
		boolean returnValue = false;

		/* Check for app */
		if (line.hasOption("app")) {
			application = line.getOptionValue("app");

			if (application == null) {
				return returnValue;
			} 
		} else {
			return returnValue;
		}
		
		/* Check for version */
		if (line.hasOption("version")) {
			version = line.getOptionValue("version");

			if (version == null) {
				return returnValue;
			} 
		} else {
			return returnValue;
		}
		
		/* Chack for Beanid */
		if (line.hasOption("beanid")) {
			beanId = line.getOptionValue("beanid");

			// if (beanId == null) {
			// return returnValue;
			// }
			if (beanId == null || beanId.trim().length() < 1) {
				getBeanName = true;
			} else {
				if (beanId.indexOf(",") != -1) {
					beanIds = beanId.split(",");
					multipleBeanIds = true;
				}
			}
		} else {
			getBeanName = true;
		}

		/* Check for publish */
		if (line.hasOption("display")) {
			String displayOption = line.getOptionValue("display");

			if (displayOption != null) {
				if(Boolean.valueOf(displayOption)) {
					display = Boolean.valueOf(displayOption);
				}
			} 
		} 
		
		if(display) {
			return true;
		}

		/* Check for scope */
		if (line.hasOption("scope")) {
			scope = line.getOptionValue("scope");

			if (scope == null) {
				return returnValue;
			} 
		} else {
			return returnValue;
		}
		
		
		
		/* Check for Bean Definition File */
		if (line.hasOption("beandefxml")) {
			beanDefXml = line.getOptionValue("beandefxml");

			if (beanDefXml == null) {
				return returnValue;
			} else {
				if (beanDefXml.indexOf(",") != -1) {
					beanDefXmls = beanDefXml.split(",");
					multipleBeanDefXmls = true;
				}
			}
		} else {
			return returnValue;
		}


		/* Chack for User */
		if (line.hasOption("user")) {
			user = line.getOptionValue("user");

			if (user == null) {
				return returnValue;
			}
		} else {
			return returnValue;
		}
		
		/* Check for publish */
		if (line.hasOption("publish")) {
			String publishOption = line.getOptionValue("publish");

			if (publishOption != null) {
				if(!Boolean.valueOf(publishOption)) {
					publish = Boolean.valueOf(publishOption);
				}
			} 
		} 
		
		

		return !returnValue;
	}
	
	private boolean getOtherOptionValuesForDisplay(CommandLine line) {
		boolean returnValue = false;

		/* Check for app */
		if (line.hasOption("app")) {
			application = line.getOptionValue("app");

			if (application == null) {
				return returnValue;
			} 
		} else {
			return returnValue;
		}
		
		/* Check for version */
		if (line.hasOption("version")) {
			version = line.getOptionValue("version");

			if (version == null) {
				return returnValue;
			} 
		} else {
			return returnValue;
		}
		
		/* Chack for Beanid */
		if (line.hasOption("beanid")) {
			beanId = line.getOptionValue("beanid");

			// if (beanId == null) {
			// return returnValue;
			// }
			if (beanId == null || beanId.trim().length() < 1) {
				getBeanName = true;
			} else {
				if (beanId.indexOf(",") != -1) {
					beanIds = beanId.split(",");
					multipleBeanIds = true;
				}
			}
		} else {
			getBeanName = true;
		}

		/* Check for publish */
		if (line.hasOption("display")) {
			String displayOption = line.getOptionValue("display");

			if (displayOption != null) {
				if(Boolean.valueOf(displayOption)) {
					display = Boolean.valueOf(displayOption);
				}
			} 
		} 
		
		return !returnValue;
	}
	
	
	private boolean getOtherOptionValuesForDelete(CommandLine line) {
		boolean returnValue = false;

		/* Check for app */
		if (line.hasOption("app")) {
			application = line.getOptionValue("app");

			if (application == null) {
				return returnValue;
			} 
		} else {
			return returnValue;
		}
		
		/* Check for version */
		if (line.hasOption("version")) {
			version = line.getOptionValue("version");

			if (version == null) {
				return returnValue;
			} 
		} else {
			return returnValue;
		}
		
		/* Chack for Beanid */
		if (line.hasOption("beanid")) {
			beanId = line.getOptionValue("beanid");
			if (beanId == null) {
				return returnValue;
			}
		} else {
			return returnValue;
		}
		

		/* Check for publish */
		if (line.hasOption("delete")) {
			String deleteOption = line.getOptionValue("delete");

			if (deleteOption != null) {
				if(Boolean.valueOf(deleteOption)) {
					delete = Boolean.valueOf(deleteOption);
				}
			} 
		} 
		
		/* Check for reupload */
		if (line.hasOption("reupload")) {
			String reuploadOption = line.getOptionValue("reupload");

			if (reuploadOption != null) {
				if(!Boolean.valueOf(reuploadOption)) {
					reupload = Boolean.valueOf(reuploadOption);
				}
			} 
		} 
		
		return !returnValue;
	}
	
	public static void main(String[] args) {
		ConfigUploaderToMongo uploader = null;
		try {
			uploader = new ConfigUploaderToMongo(args);
			
		} catch (Exception e) {
			LOGGER.info( "Fatal Error : Config Exception" +e.getMessage(), e);
			System.exit(1);
		}

		try {
			if (!uploader.invalidOption) {
				if(uploader.isDisplay()) {
					uploader.display();
				} else if(uploader.isDelete()) {
					uploader.delete();
				} else {
					uploader.upload();	
				}
				
			} else {
				LOGGER.info(
						"Fatal Error : Options/Arguments passed to Application are invalid.");
			}
		} catch (Exception e) {
			LOGGER.info( "Fatal Error : Config Publish Exception" +e.getMessage(), e);
			System.exit(1);
		}

		System.exit(0);

	}

}