/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.netflix.curator.utils.ZookeeperFactory;


/**
 * 
 * @author rmuthupandian
 *
 */
public class ZooKeeperFactory implements ZookeeperFactory {

	Long sessionId ;
	byte[] sessionPwd ;
	
	// previous sessionId is available... Reconnecting to same session
	public ZooKeeperFactory(long sessionId, byte[] sessionpwd) {
		this.sessionId = sessionId;
		this.sessionPwd = sessionpwd;
	}
	
	// if no sessionId is available...
	public ZooKeeperFactory() {
	
	}

	@Override
	public ZooKeeper newZooKeeper(String connectString, int sessionTimeout,
			Watcher watcher, boolean canBeReadOnly) throws Exception {
		if(sessionId != null && sessionPwd.length > 0){
			return new ZooKeeper(connectString, sessionTimeout,watcher, sessionId, sessionPwd, canBeReadOnly);
		} else {
			return new ZooKeeper(connectString, sessionTimeout,watcher,  canBeReadOnly);
		}
	}

}
