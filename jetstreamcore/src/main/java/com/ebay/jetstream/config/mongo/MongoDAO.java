/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class MongoDAO {

      public static List<JetStreamBeanConfigurationDo> findConfigurationByAppNameAndVersion(BasicDBObject query, MongoConnection mongoConnection) {
    	  
    	List<JetStreamBeanConfigurationDo> beanConfigs = new ArrayList<JetStreamBeanConfigurationDo>();
    	List<BasicDBObject> dbObjects = new ArrayList<BasicDBObject>();
  		DBCollection dbCol = mongoConnection.getDBCollection();
  		
  		if (dbCol == null) {
  			throw new MongoConfigRuntimeException("jetstreamconfig collection is unknown");
  		}
  		
  		Exception e = null;
  		DBCursor cur = null;
  		try {
  			cur = (query == null ? dbCol.find() : dbCol.find(query));
  			while(cur.hasNext()) {
  	        	dbObjects.add( (BasicDBObject)cur.next() );
  	    	}
  	    	
  			for(BasicDBObject dbObject : dbObjects) {
  				String jsonString = dbObject.toString();
  	  	    	beanConfigs.add (unMarshalJSONResponse(jsonString) );	
  			}
  	    } catch (Exception err) {
  			e = err;
  			throw new MongoConfigRuntimeException(err);
  		} finally {
  		    if (cur != null) {
  		      cur.close();
  		    }
  		}

  		return beanConfigs;
  	}
      
    public static List<JetStreamBeanConfigurationDo> findConfigurationByQuery(BasicDBObject query, MongoConnection mongoConnection) {
	  
	  	List<JetStreamBeanConfigurationDo> beanConfigs = new ArrayList<JetStreamBeanConfigurationDo>();
	  	List<BasicDBObject> dbObjects = new ArrayList<BasicDBObject>();
		DBCollection dbCol = mongoConnection.getDBCollection();
		
		if (dbCol == null) {
			throw new MongoConfigRuntimeException("jetstreamconfig collection is unknown");
		}
		
		Exception e = null;
		
		DBCursor cur = null;
		try {
			cur = (query == null ? dbCol.find() : dbCol.find(query));
			while(cur.hasNext()) {
	        	dbObjects.add( (BasicDBObject)cur.next() );
	    	}
	    	
			for(BasicDBObject dbObject : dbObjects) {
				String jsonString = dbObject.toString();
	  	    	beanConfigs.add (unMarshalJSONResponse(jsonString) );	
	    		//beanConfig = (JetStreamBeanConfigurationDo)fromJson(jsonString, JetStreamBeanConfigurationDo.class);
			}
	    } catch (Exception err) {
			e = err;
			throw new MongoConfigRuntimeException(err);
		} finally {
		    if (cur != null) {
		        cur.close();
		    }
		}

		return beanConfigs;
	}
      
    private static JetStreamBeanConfigurationDo unMarshalJSONResponse(
            String jsonString) {
        try {
            JetStreamBeanConfigurationDo result = null;
            if (jsonString != null) {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    result = mapper.readValue(jsonString,
                            JetStreamBeanConfigurationDo.class);
                } catch (Exception e) {
                    throw new MongoConfigRuntimeException(e);
                }
            }

            return result;

        } catch (Throwable e) {
            throw new MongoConfigRuntimeException(e);
        }
    }
    
    
    /**
     * UPLOAD TO DB
     */
    public static void insertJetStreamConfiguration(BasicDBObject dbObject, MongoConnection mongoConnection) {
  	  	JetStreamBeanConfigurationDo beanConfig = null;
  	  	DBCollection dbCol = mongoConnection.getDBCollection();
		
		if (dbCol == null) {
			throw new MongoConfigRuntimeException("jetstreamconfig collection is unknown");
		}
		

		WriteResult result =  dbCol.insert(dbObject);
		if (result.getError() != null) {
			throw new MongoConfigRuntimeException(result.getError());
		}
	}
    
    
    public static boolean removeConfigurationByQuery(BasicDBObject query, MongoConnection mongoConnection) {
  	  
	  	DBCollection dbCol = mongoConnection.getDBCollection();
		
		if (dbCol == null) {
			throw new MongoConfigRuntimeException("jetstreamconfig collection is unknown");
		}
		
		try {
			if(query ==null) {
				return false;
			}
			
			WriteResult result = dbCol.remove(query, WriteConcern.SAFE);
			
			if(result.getLastError().ok()) {
				return true;
			}
			
	    } catch (Exception err) {
			throw new MongoConfigRuntimeException(err);
		} 

		return true;
	}
  }