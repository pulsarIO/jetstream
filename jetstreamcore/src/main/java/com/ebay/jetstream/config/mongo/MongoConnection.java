/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.config.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
  
 public class MongoConnection {
  
	  private static final String collection = "jetstreamconfig";
      private DB db = null;
      
      public MongoConnection(MongoConfiguration mongoConfiguration) {
    	  
          List<ServerAddress> hosts = new ArrayList<ServerAddress>();
          for (String host : mongoConfiguration.getHosts()) {
              try {
                  hosts.add(new ServerAddress(host, Integer.valueOf(mongoConfiguration.getPort())));
              } catch (UnknownHostException e) {
              }
          }
  
          Mongo mongo = null;
          mongo = new Mongo(hosts);
          db = mongo.getDB( mongoConfiguration.getDb());
          
          authenticate(mongoConfiguration);
      }
  
      public MongoConnection(List<String> hostStrings, String port, String database) {
  
          List<ServerAddress> hosts = new ArrayList<ServerAddress>();
          for (String host : hostStrings) {
              try {
                  hosts.add(new ServerAddress(host, Integer.valueOf(port)));
              } catch (UnknownHostException e) {
              }
          }
  
          Mongo mongo = null;
          mongo = new Mongo(hosts);
          db = mongo.getDB(database);
          
      }
  
      public DB getDB(){
          return db;
      }
      
      public DBCollection getDBCollection() {
  		return (db == null ? null : db.getCollection(collection) );
  	  }
      
      private void authenticate(MongoConfiguration mongoConfiguration) {
    	  if(mongoConfiguration.getUser() != null && mongoConfiguration.getUser().length() > 0) {
          	  if(mongoConfiguration.getPw() != null && mongoConfiguration.getPw().length() > 0) {
          		  Boolean auth = db.authenticate(mongoConfiguration.getUser(), mongoConfiguration.getPw().toCharArray());
                    if (!auth)
                        throw new MongoConfigRuntimeException("Mongo Authentication Failed");  
          	  }
            }
      }
      
  }