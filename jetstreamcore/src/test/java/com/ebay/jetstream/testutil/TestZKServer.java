/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.testutil;

import com.netflix.curator.test.TestingCluster;

public class TestZKServer {

	static TestingCluster zkcluster;
	static String cnxnString;
	static String[] hostAndPort;
	
	static {

		zkcluster = new TestingCluster(1);
		cnxnString = zkcluster.getConnectString();
		System.out.println("Zookeeper Servers :" + cnxnString);
		hostAndPort = cnxnString.split(":");

		try {
			zkcluster.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static String getCnxnString(){
		return cnxnString;
	}
	
	public  static String getPort(){
		return hostAndPort[1];
	}

}
