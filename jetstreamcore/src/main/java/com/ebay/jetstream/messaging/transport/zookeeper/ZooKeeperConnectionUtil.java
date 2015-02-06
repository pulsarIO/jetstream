/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;


import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.utils.ZookeeperFactory;

public class ZooKeeperConnectionUtil {

	private RetryPolicy retryPolicy;
	
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}
	
	public static CuratorFramework createZookeeperConnection(String connectString,
			int sessionTimeoutMs, int connectionTimeoutMs, ConnectionStateListener cnxnListener,
			int retrycount, int retryWaitTime, long sessionId, byte[] sessionPwd) throws Exception {
		
		RetryPolicy	retryPolicy = new RetryNTimes(retrycount , retryWaitTime);
			
		ZookeeperFactory factory = new ZooKeeperFactory(sessionId, sessionPwd);
		
		CuratorFramework client = CuratorFrameworkFactory.builder().zookeeperFactory(factory)
				.sessionTimeoutMs(sessionTimeoutMs)
				.connectionTimeoutMs(connectionTimeoutMs)
				.connectString(connectString).retryPolicy(retryPolicy).build();
				
		client.getConnectionStateListenable().addListener(cnxnListener);
		client.start();
		
		return client;
	}
	
	
	public static CuratorFramework createZookeeperConnection(String connectString,
			int sessionTimeoutMs, int connectionTimeoutMs, ConnectionStateListener cnxnListener,
			int retrycount, int retryWaitTime)  {
		
		RetryPolicy	retryPolicy = new RetryNTimes(retrycount , retryWaitTime);
				
		CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
				
		client.getConnectionStateListenable().addListener(cnxnListener);
		try{
			client.start();
		}catch(Exception e ){
			//swallow it
		}
		
		return client;
	}

}
