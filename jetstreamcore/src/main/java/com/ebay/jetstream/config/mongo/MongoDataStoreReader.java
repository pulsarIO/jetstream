/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinitionReader;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import com.ebay.jetstream.config.BeanChangeInformation;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.spring.beans.factory.support.UpdateableListableBeanFactory;

/**
 * MongoDataStoreReader is a class which reads bean information from Mongo
 * database and uses Spring API's to register the beans.
 * 
 */

public class MongoDataStoreReader extends AbstractBeanDefinitionReader {

	private static final String CN_VALUE = "cn";
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDataStoreReader.class.getPackage().getName());
//	private String m_app;
//	private String m_scope;
//	private String m_version;
	private MongoDbResource m_mongoDbResource;
	private final Map<String, String> m_scopeMap = new ConcurrentHashMap<String, String>();
	private final List<String> m_scopeList = new Vector<String>();

	/* A HashMap which stores beanName as the Key and scope as its value */
	private final Map<String, BeanInformation> m_beanInformation = new ConcurrentHashMap<String, BeanInformation>();


	/**
	 * 
	 * @param beanFactory
	 */
	public MongoDataStoreReader(BeanDefinitionRegistry beanFactory) {
		super(beanFactory);
	}

	/**
	 * 
	 * @param searchResult
	 * @return
	 */
	private boolean checkIfBeanAlreadyRegistered(SearchResult searchResult)
			throws NamingException {
		Attributes attributes = searchResult.getAttributes();

		Attribute beanNameAttribute = attributes.get(CN_VALUE);

		/* Get Bean name */
		if (beanNameAttribute != null) {
			String beanName;
			beanName = (String) beanNameAttribute.get();
			boolean result = getBeanFactory().containsBeanDefinition(beanName);

			if (result == true) {
				if (LOGGER.isInfoEnabled()) {
					String msg = "In checkIfBeanAlreadyRegistered method : "
							+ beanName + " already registered with" + " scope "
							+ m_beanInformation.get(beanName);
					LOGGER.info( msg + "\n");
				}
			}

			return result;
		}
		return false;
	}

	public Map<String, BeanInformation> getBeanInformation() {
		return Collections.unmodifiableMap(m_beanInformation);
	}

	private String getBeanScope(boolean isSingleton) {
		return isSingleton ? BeanDefinition.SCOPE_SINGLETON
				: BeanDefinition.SCOPE_PROTOTYPE;
	}

	public List<String> getScopeList() {
		return m_scopeList;
	}

	public Map<String, String> getScopeMap() {
		return m_scopeMap;
	}

	/**
	 * 
	 * @param beanName
	 * @param application
	 * @param scope
	 * @param version
	 * @return
	 */
	public boolean isUpdateable(BeanChangeInformation beanChangeInformation) {
		String beanNameThatChanged = beanChangeInformation.getBeanName();
		String changedBeanApplication = beanChangeInformation.getApplication();
		String changedBeanScope = beanChangeInformation.getScope();
		String changedBeanAppVersion = beanChangeInformation.getVersionString();
		String changedBeanVersion = beanChangeInformation.getBeanVersion();
		
		BeanInformation existingBeanInformation = m_beanInformation.get(beanNameThatChanged);
		
		// Check Application
		String appName = m_mongoDbResource.getConfiguration().getApplicationInformation().getApplicationName();
		if (!(appName != null && appName.equals(changedBeanApplication))) {
			return false;
		}

		// Check version
		String version = m_mongoDbResource.getConfiguration().getApplicationInformation().getConfigVersion();
		if (!version.equals(changedBeanAppVersion)) {
			return false;
		}
		
		if(existingBeanInformation == null) {
			//return true;
			return isCorrectScope(changedBeanScope);
		}
		
		// priority is like this
	    // local > dc (phx/slc,lvs) > global
	    
		boolean eligible = false;
	    // if it is same scope, pick latest bean version
		
		// is existing local scope
		if( existingBeanInformation.getScope().startsWith(ConfigScope.local.name()) ) {
			if(changedBeanScope.startsWith(ConfigScope.local.name())) {
				if(MongoScopesUtil.isLocalEligible(changedBeanScope)) {
					eligible = true;	
				} else {
					return false;
				}
				
			} else {
				return false;
			}
		} 
		// is existing dc scope
		else if( existingBeanInformation.getScope().startsWith(ConfigScope.dc.name()) ) {
			if(changedBeanScope.startsWith(ConfigScope.dc.name())) {
				if(MongoScopesUtil.isDCEligible(changedBeanScope)) {
					eligible = true;	
				} else {
					return false;
				}
			}  else if(changedBeanScope.startsWith(ConfigScope.local.name())) {
				if(MongoScopesUtil.isLocalEligible(changedBeanScope)) {
					eligible = true;	
				} else {
					return false;
				}
			}  
			else {
				//return true;
				eligible = true;
			}
		}  
		// is existing global scope
		else if( existingBeanInformation.getScope().equals(ConfigScope.global.name()) ) {
			if(changedBeanScope.equals(ConfigScope.global.name())) {
					eligible = true;	
			} else if(changedBeanScope.startsWith(ConfigScope.local.name())) {
				if(MongoScopesUtil.isLocalEligible(changedBeanScope)) {
					eligible = true;	
				} else {
					return false;
				}
			} else if(changedBeanScope.startsWith(ConfigScope.dc.name())) {
				if(MongoScopesUtil.isDCEligible(changedBeanScope)) {
					eligible = true;	
				} else {
					return false;
				}
			}
			else {
				return false;
			}
		}
		
		if(eligible) {
			int loadedBeanVerion = Integer.valueOf(existingBeanInformation.getBeanVersion());
			int changedBeanVerion = Integer.valueOf(changedBeanVersion);
			if(loadedBeanVerion >= changedBeanVerion) {
				return false;
			}
		}
		
		// this check is redundant
//		else if(existingBeanInformation.getScope().equals(ConfigScope.global.name())) {
//			if(!changedBeanScope.equals(ConfigScope.global.name())) {
//				return true;
//			} else {
//				return false;
//			}
//		} 

		return true;
	}
	
	public boolean updateBean(BeanChangeInformation beanChangeInformation) throws NamingException {
		boolean isSingleton = false;
		boolean newBean = false;
		boolean actuallyUpdate = false;
		boolean failed = false;
		String failureLog = null;

		if (LOGGER.isInfoEnabled()) {
			LOGGER.info( "Entry into updateBean Method");
		}
		
		List<JetStreamBeanConfigurationDo> beanConfigs = m_mongoDbResource.getMongoConfigMgr().getJetStreamConfiguration(
				beanChangeInformation.getApplication(), beanChangeInformation.getVersionString(),
				beanChangeInformation.getBeanName(), beanChangeInformation.getBeanVersion(),
				beanChangeInformation.getScope());
		
		for(JetStreamBeanConfigurationDo beanConfig : beanConfigs){
			
			UpdateableListableBeanFactory beanFactory = (UpdateableListableBeanFactory) getBeanFactory();
			String beanName = beanChangeInformation.getBeanName();
			String scope = beanChangeInformation.getScope();
			beanFactory.addBeanForUpdate(beanName);
			
			try {

				// Check if a bean exits in the container
				if (beanFactory.containsBean(beanName)) {
					isSingleton = beanFactory.isSingleton(beanName);

					if (LOGGER.isInfoEnabled()) {
						LOGGER.info( "beanName " + beanName + " is a "+ (isSingleton ? "Singleton" : "Prototype")+ " bean");
						LOGGER.info( "scope : " + scope);
					}

					m_beanInformation.remove(beanName);
					beanFactory.removeBeanDefinition(beanName);
					beanFactory.destroySingleton(beanName);
					Management.removeBeanOrFolder(beanName);
				} else {
					newBean = true;
				}

				
			
				loadBeanDefinitionsFromString(beanConfig.getBeanDefinition());
//	
//						
//						/* Register with Spring */
//						BeanDefinitionReaderUtils.registerBeanDefinition(
//								beanDefinitionHolder, beanFactory);

					
				/* Store the info in the hashMap */
				m_beanInformation.put(beanConfig.getBeanName(), beanConfig.getBeanInformation());

				if (newBean) {
					isSingleton = getBeanFactory().getBeanDefinition(beanConfig.getBeanName()).isSingleton();
				}

				actuallyUpdate = true;
					
				
				
				if (isSingleton) {
					beanFactory.preInstantiateSingletons();
					beanFactory.getBean(beanName);
					//beanFactory.preInstantiateSingletons()
				}

				
			} catch(Exception e) {
				failed = true;
				failureLog = e.getMessage();
				LOGGER.error( "  Exception in updateBean method of dynamic config update", e);
			}
			finally {
				try {
					if(failed) {
						beanFactory.removeBeanDefinition(beanName);	
						beanFactory.destroySingleton(beanName);
					}
					
					beanFactory.updateComplete();	
				} catch(Exception e) {
					failed = true;
					failureLog = e.getMessage();
					LOGGER.error( "  Exception in updateBean method finally block of dynamic config update", e);
				}
				
			}
			
			addDetailsToManagement(beanConfig, failed, failureLog);
		}

		if(failed) {
			return false;
		}

		return true;
	}

	// /**
	// *
	// * @param dirContext
	// * @return int
	// * @throws BeanDefinitionStoreException
	// */
	// private int loadBeanDefinitions(DirContext dirContext) throws
	// BeanDefinitionStoreException, NamingException {
	// long startTime = System.currentTimeMillis() / 1000;
	// int countBefore = 0;
	// int countAfter = 0;
	//
	// countBefore = getBeanFactory().getBeanDefinitionCount();
	//
	// try {
	// if (loadBeanDefinitionsFromScopeInfo(dirContext) == false) {
	// return 0;
	// }
	//
	// countAfter = getBeanFactory().getBeanDefinitionCount();
	//
	// long endTime = System.currentTimeMillis() / 1000;
	// if (LOGGER.isInfoEnabled()) {
	// String msg = "Time taken to load from LDAP server is : " + (endTime -
	// startTime);
	// LOGGER.info( msg);
	// }
	//
	// }
	// finally {
	// if (dirContext != null)
	// dirContext.close();
	// }
	//
	// return countAfter - countBefore;
	// }

	/**
	 * 
	 * @param Resource
	 * @return int
	 */
	public int loadBeanDefinitions(Resource mongoResource) throws BeanDefinitionStoreException {
		if (mongoResource instanceof MongoDbResource) {
			m_mongoDbResource = (MongoDbResource) mongoResource;
			long startTime = System.currentTimeMillis() / 1000;
			int countBefore = 0;
			int countAfter = 0;
			boolean failed = false;
			String failureLog = null;


			countBefore = getBeanFactory().getBeanDefinitionCount();
			int countOfBeanConfigsFromDB = 0;

			try {
				
				List<JetStreamBeanConfigurationDo> beanConfigs = m_mongoDbResource.getMongoConfigMgr().getJetStreamConfiguration(
						m_mongoDbResource.getConfiguration().getApplicationInformation().getApplicationName(),
						m_mongoDbResource.getConfiguration().getApplicationInformation().getConfigVersion());
		
				for (JetStreamBeanConfigurationDo beanConfig : beanConfigs) {
					if (beanConfig.getBeanDefinition() != null) {
						try {
							if (!isCorrectScope(beanConfig.getScope())) {
								continue;
							}
							
							loadBeanDefinitionsFromString(beanConfig.getBeanDefinition());
							countOfBeanConfigsFromDB++;
							
							/* Store the info in the HashMap */
							m_beanInformation.put(beanConfig.getBeanName(),beanConfig.getBeanInformation());
							
							
							addDetailsToManagement(beanConfig, failed, failureLog);
						} catch (Exception e) {
							failed = true;
							failureLog = e.getMessage();
							addDetailsToManagement(beanConfig, failed, failureLog);
							
							LOGGER.info( "Error loading bean defition :  \n "+ beanConfig.toString() + " \n ", e);
						}
					}
				}
		
				countAfter = getBeanFactory().getBeanDefinitionCount();
		
				long endTime = System.currentTimeMillis() / 1000;
				if (LOGGER.isInfoEnabled()) {
					String msg = "Time taken to load from Mongo server is : "+ (endTime - startTime + " and number of beans loaded are : " +(countOfBeanConfigsFromDB));
					LOGGER.info( msg);
				}
				
			} catch(Throwable e) {
				LOGGER.info( "Error loading bean defition :  \n ", e);
			}
			
			
			return ( ((countAfter-countBefore) == countOfBeanConfigsFromDB) ? (countAfter-countBefore) : countOfBeanConfigsFromDB );

		}

		return 0;
	}
	
	private boolean isCorrectScope(String scope) {
		boolean result = true;

		if (scope.startsWith(ConfigScope.local.name())) {
			if (!MongoScopesUtil.isLocalEligible(scope)) {
				result = false;
			}
		} else if (scope.startsWith(ConfigScope.dc.name())) {
			if (!MongoScopesUtil.isDCEligible(scope)) {
				result = false;
			}
		} 
		
		return result;
	}
	
	private void addDetailsToManagement(JetStreamBeanConfigurationDo beanConfig, boolean failed, String failureLog) {
		try {
			MongoConfigDetails mongoConfigDetail = new MongoConfigDetails();
			mongoConfigDetail.setApplication(beanConfig.getAppName());
			mongoConfigDetail.setApplicationVersion(beanConfig.getVersion());
			mongoConfigDetail.setBeanName(beanConfig.getBeanName());
			mongoConfigDetail.setBeanVersion(beanConfig.getBeanVersion());
			mongoConfigDetail.setUser(beanConfig.getModifiedBy());
			Date modifiedDate = new Date(beanConfig.getModifiedDate());
			mongoConfigDetail.setUpdateTime(modifiedDate);
			
			if(failed) {
				if(failureLog != null) {
					mongoConfigDetail.setLog(failureLog);	
				}
			}
			m_mongoDbResource.addToManagement(mongoConfigDetail, failed);
		} catch(Exception e) {
			LOGGER.info( "MongoDataStoreReader.addDetailsToManagement() - Error adding bean defition to management console ", e);
		}
		
	}

	private int loadBeanDefinitionsFromString(String beanDefinition) {
		AbstractBeanDefinitionReader beanDefinitionReader = new XmlBeanDefinitionReader(this.getRegistry());
		Resource resource = new ByteArrayResource(beanDefinition.getBytes());
		int count = beanDefinitionReader.loadBeanDefinitions(resource);

		return count;
	}


}
