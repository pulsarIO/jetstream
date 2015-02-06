/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;

public class MongoConfigMgr {
	
	private static MongoConnection mongoConnection;
	
	@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification="Okay in this release, will look at fixing later.")
	public MongoConfigMgr(MongoConfiguration mongoConfiguration) throws Exception {
		 mongoConnection = new MongoConnection(mongoConfiguration); // FIXME
	}
	
	
	/**
	   *  READ FROM DB
	   */

    public List<JetStreamBeanConfigurationDo> getJetStreamConfiguration(String appName, String version) {
        BasicDBObject query = new BasicDBObject();
        query.put("appName", appName);
        query.put("version", version);
        
        List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO.findConfigurationByAppNameAndVersion(query, mongoConnection);
        
        List<JetStreamBeanConfigurationDo> scopedBeanConfigs = getBeanConfigsBasedOnScope(beanConfigs);
    
        return scopedBeanConfigs;
    }
    
    public List<JetStreamBeanConfigurationDo> getJetStreamConfiguration(String appName, String version, String beanName) {
        BasicDBObject query = new BasicDBObject();
        query.put("appName", appName);
        query.put("version", version);
        query.put("beanName", beanName);
        
        List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO.findConfigurationByAppNameAndVersion(query, mongoConnection);
        
        List<JetStreamBeanConfigurationDo> scopedBeanConfigs = getBeanConfigsBasedOnScope(beanConfigs);
    
        return scopedBeanConfigs;
    }
    
    public List<JetStreamBeanConfigurationDo> getJetStreamConfiguration(String appName, String version, String beanName,
    																	String beanVersion, String scope) {
        BasicDBObject query = new BasicDBObject();
        query.put("appName", appName);
        query.put("version", version);
        query.put("beanName", beanName);
        query.put("beanVersion", beanVersion);
        query.put("scope", scope);
        
        
        List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO.findConfigurationByAppNameAndVersion(query, mongoConnection);
        
        List<JetStreamBeanConfigurationDo> scopedBeanConfigs = getBeanConfigsBasedOnScope(beanConfigs);
    
        return scopedBeanConfigs;
    }
    
    public JetStreamBeanConfigurationDo getJetStreamConfiguration(String appName, String version, String beanName,
																		String scope) {
		BasicDBObject query = new BasicDBObject();
		query.put("appName", appName);
		query.put("version", version);
		query.put("beanName", beanName);
		query.put("scope", scope);
		
		
		List<JetStreamBeanConfigurationDo> beanConfigs = MongoDAO.findConfigurationByQuery(query, mongoConnection);
		
		JetStreamBeanConfigurationDo latestBeanConfig = getLatestBeanConfig(beanConfigs);
		
		return latestBeanConfig;
    }
    
    private JetStreamBeanConfigurationDo getLatestBeanConfig(List<JetStreamBeanConfigurationDo> beanConfigs) {
    	JetStreamBeanConfigurationDo latestBeanConfig = null;
    	
    	for(JetStreamBeanConfigurationDo beanConfig : beanConfigs) {
    		if(latestBeanConfig == null) {
    			latestBeanConfig = beanConfig;
    		} else {
    			if(latestBeanConfig.getBeanVersion() != null && beanConfig.getBeanVersion() != null) {
    				if(Integer.valueOf(latestBeanConfig.getBeanVersion()) < Integer.valueOf(beanConfig.getBeanVersion())) {
    					latestBeanConfig = beanConfig;
    				}
    			}
    		}
    	}
    	
    	return latestBeanConfig;
    }
    
    private List<JetStreamBeanConfigurationDo> getBeanConfigsBasedOnScope(List<JetStreamBeanConfigurationDo> beanConfigs) {
    	List<JetStreamBeanConfigurationDo> scopedBeanConfigs = new ArrayList<JetStreamBeanConfigurationDo>();
    	Map<String,JetStreamBeanConfigurationDo> scopedMap = new HashMap<String,JetStreamBeanConfigurationDo>();
    	
    	for(JetStreamBeanConfigurationDo beanConfig : beanConfigs) {
    		if( scopedMap.isEmpty() ) {
    			scopedMap.put(beanConfig.getBeanName(), beanConfig);
    		} else {
    			if( scopedMap.containsKey(beanConfig.getBeanName()) ) {
    				if( replaceOneInMap((JetStreamBeanConfigurationDo)scopedMap.get(beanConfig.getBeanName()), beanConfig) ){
    					scopedMap.put(beanConfig.getBeanName(), beanConfig);	
    				}
    			} else {
    				scopedMap.put(beanConfig.getBeanName(), beanConfig);
    			}
    		}
    	}
    	
    	Iterator iter = scopedMap.keySet().iterator();
    	while(iter.hasNext()) {
    		scopedBeanConfigs.add( (JetStreamBeanConfigurationDo)scopedMap.get( (String)iter.next() ) );
    	}
    	
    	return scopedBeanConfigs;
    }
    
    // priority is like this
    // local > dc (phx/slc,lvs) > global
    
    // if it is same scope, pick latest bean version
    private boolean replaceOneInMap(JetStreamBeanConfigurationDo oneInMap, JetStreamBeanConfigurationDo currentOne) {
        boolean replace = false;
        
        if(oneInMap.getScope().startsWith(ConfigScope.local.name())) {
            //oneInMap not current host
            if(!MongoScopesUtil.isLocalEligible(oneInMap.getScope())){
                if (MongoScopesUtil.isLocalEligible(currentOne.getScope())
                        || MongoScopesUtil.isDCEligible(currentOne.getScope())
                        || currentOne.getScope().equals(ConfigScope.global.name())) {
                    replace = true;
                }
            }else if(MongoScopesUtil.isLocalEligible(currentOne.getScope())) {
                // same scope but pick latest version
                replace = shouldWeReplaceBasedOnVersion(oneInMap.getBeanVersion(), currentOne.getBeanVersion());  
            }
        } else if(oneInMap.getScope().startsWith(ConfigScope.dc.name())) {
            //oneInMap not current dc
            if(!MongoScopesUtil.isDCEligible(oneInMap.getScope())){
                if (MongoScopesUtil.isLocalEligible(currentOne.getScope())
                        || (MongoScopesUtil.isDCEligible(currentOne.getScope()) 
                        || currentOne.getScope().equals(ConfigScope.global.name()))) {
                    replace = true;
                }
            }
            else if(MongoScopesUtil.isDCEligible(currentOne.getScope())) {
                // same scope but pick latest version
                replace = shouldWeReplaceBasedOnVersion(oneInMap.getBeanVersion(), currentOne.getBeanVersion());
            }else if(MongoScopesUtil.isLocalEligible(currentOne.getScope())){
                replace = true;
            }
        } else if(oneInMap.getScope().equals(ConfigScope.global.name()) ) {
            if(!currentOne.getScope().equals(ConfigScope.global.name()) ) {
                if( MongoScopesUtil.isLocalEligible(currentOne.getScope()) || MongoScopesUtil.isDCEligible(currentOne.getScope()) ) {
                    replace = true; 
                }
            } else {
                replace = shouldWeReplaceBasedOnVersion(oneInMap.getBeanVersion(), currentOne.getBeanVersion());
            }
        }
        return replace;
    }


    
    private boolean shouldWeReplaceBasedOnVersion(String versionInMap, String currentVersion) {
    	if(versionInMap != null && currentVersion != null) {
			if( Double.valueOf(versionInMap) < Double.valueOf(currentVersion) ) {
    			return true;
    		}
		}
    	
    	return false;
    }
    
    
    public void uploadJetStreamConfiguration(JetStreamBeanConfigurationDo beanConfigDo) {
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("appName", beanConfigDo.getAppName());
        dbObject.put("version", beanConfigDo.getVersion());
        dbObject.put("scope", beanConfigDo.getScope());
        
        dbObject.put("beanDefinition", beanConfigDo.getBeanDefinition());
        dbObject.put("beanVersion", beanConfigDo.getBeanVersion());
        dbObject.put("beanName", beanConfigDo.getBeanName());
        
        
        dbObject.put("createdBy", beanConfigDo.getCreatedBy());
        dbObject.put("modifiedBy", beanConfigDo.getModifiedBy());
        
        dbObject.put("creationDate", beanConfigDo.getCreationDate());
        dbObject.put("modifiedDate", beanConfigDo.getModifiedDate());
        
        
        
        MongoDAO.insertJetStreamConfiguration(dbObject, mongoConnection);
    }
    
    public boolean removeJetStreamConfiguration(String appName, String version, String beanName, String beanVersion, String scope) {
        BasicDBObject query = new BasicDBObject();
        query.put("appName", appName);
        query.put("version", version);
        query.put("beanName", beanName);
        query.put("beanVersion", beanVersion);
        query.put("scope", scope);
        
        boolean result = MongoDAO.removeConfigurationByQuery(query, mongoConnection);
        
        return result;
    }

}