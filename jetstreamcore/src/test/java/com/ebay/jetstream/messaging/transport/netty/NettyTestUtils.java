/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.ebay.jetstream.config.ConfigException;
import com.ebay.jetstream.config.NICUsage;
import com.ebay.jetstream.messaging.config.ContextConfig;
import com.ebay.jetstream.messaging.config.MessageServiceProperties;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperNode;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperTransportConfig;
import com.netflix.curator.test.TestingCluster;

public class NettyTestUtils {
	
	static TestingCluster zkcluster;
	static String cnxnString;
	static String[] hostAndPort;
	static NICUsage nicusage = null;
	static MessageServiceProperties msp;

   static {
	   
	   zkcluster = new TestingCluster(1);
		cnxnString = zkcluster.getConnectString();
		System.out.println("Zookeeper Servers :" + cnxnString);
		hostAndPort= cnxnString.split(":");
		

		try {
			nicusage = new NICUsage();
		} catch (ConfigException e) {
			assertTrue(e.getLocalizedMessage(), true);
		}
	
		msp = new MessageServiceProperties();
		msp.setNicUsage(nicusage);

		try {
			zkcluster.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
   }
   
   
   public static MessageServiceProperties getMsp() {
	return msp;
}

static boolean done= false;
   static ZooKeeperTransportConfig zookeeperconfig = new ZooKeeperTransportConfig();
   
   public  static ZooKeeperTransportConfig useZookeeper(){
		if (done)
			return zookeeperconfig;
		else{
			// lets create zookeeper transport config
	
			zookeeperconfig.setProtocol("tcp");
			
			// now create context config
			ArrayList<ContextConfig> zookeepercontexts = new ArrayList<ContextConfig>();
			ContextConfig cc1 = new ContextConfig();
			cc1.setContextname("Rtbdpod.Messaging");
			
			zookeepercontexts.add(cc1);
			ContextConfig cc2 = new ContextConfig();
			cc2.setContextname("Rtbdpod.Local");
			
			zookeepercontexts.add(cc2);
			zookeeperconfig.setContextList(zookeepercontexts);
			zookeeperconfig
					.setTransportClass("com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperTransport");
			zookeeperconfig.setTransportName("zookeeper");
			List<ZooKeeperNode> zknodes = new ArrayList<ZooKeeperNode>();
	
			ZooKeeperNode zk = new ZooKeeperNode();
			zk.setHostname(hostAndPort[0]);
			zk.setPort(Integer.parseInt(hostAndPort[1]));
			zknodes.add(zk);
			zookeeperconfig.setZknodes(zknodes);
			done=true;
			return zookeeperconfig;
		}
	}
	

   
   
	
}
