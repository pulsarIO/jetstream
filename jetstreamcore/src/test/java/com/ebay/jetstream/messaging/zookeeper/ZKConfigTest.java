/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.zookeeper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperNode;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperTransportConfig;

public class ZKConfigTest {
	
	
	@Test
	public void testSameConfigEquals(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		assertTrue(config1.equals(config2));
		
	}
	
	
	@Test
	public void testDifferentConfigEquals(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		// do some changes in config2
		config2.setCxnWaitInMillis(1111);
		
		assertFalse(config1.equals(config2));
		
	}
	
	@Test
	public void testDifferentZKNodeEquals(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		// change ZkNode in config2
		List<ZooKeeperNode> nodes = config2.getZknodes();
		ZooKeeperNode node2 = new ZooKeeperNode();
		node2.setHostname("localhost1");
		nodes.add(node2);
		
		assertFalse(config1.equals(config2));
		
	}
	
	@Test
	public void testsameMultipleZKNodeEquals(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		// change ZkNode in config2
		List<ZooKeeperNode> nodes = config2.getZknodes();
		ZooKeeperNode node2 = new ZooKeeperNode();
		node2.setHostname("localhost1");
		node2.setPort(2181);
		nodes.add(node2);
		
		config1.setZknodes(nodes);
		
		assertTrue(config1.equals(config2));
		
	}
	
	@Test
	public void testDifferentMultipleZKNodeEquals(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		// change ZkNode in config2
		List<ZooKeeperNode> nodes = config2.getZknodes();
		ZooKeeperNode node2 = new ZooKeeperNode();
		node2.setHostname("localhost2");
		node2.setPort(2181);
		nodes.add(node2);
		
		
		// change ZkNode in config1
			List<ZooKeeperNode> nodes1 = config2.getZknodes();
			ZooKeeperNode node12 = new ZooKeeperNode();
			node12.setHostname("localhost11");
			node12.setPort(2181);
			nodes1.add(node2);
			
		
		assertFalse(config1.equals(config2));
		
	}
	
	
	@Test
	public void testEmptyProtocolTopics(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		
		config1.setNettyDiscoveryProtocolTopics(new ArrayList<String>());
		
		assertTrue(config1.equals(config2));
		
	}
	
	@Test
	public void testSameProtocolTopics(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		
		List<String> topics= new ArrayList<String>();
		topics.add("testtopic1");
		topics.add("testtopic2");
		
		config1.setNettyDiscoveryProtocolTopics(topics);
		config2.setNettyDiscoveryProtocolTopics(topics);
		
		assertTrue(config1.equals(config2));
		
	}
	
	@Test
	public void testDifferentProtocolTopics(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		
		List<String> topics= new ArrayList<String>();
		topics.add("testtopic1");
		topics.add("testtopic2");
		
		config1.setNettyDiscoveryProtocolTopics(topics);
		
		assertFalse(config1.equals(config2));
		
	}
	
	@Test
	public void testDifferentSameSizeProtocolTopics(){
		
		ZooKeeperTransportConfig config1 = getZkTransportConfig();
		ZooKeeperTransportConfig config2 = getZkTransportConfig();
		
		
		List<String> topics= new ArrayList<String>();
		topics.add("testtopic1");
		topics.add("testtopic2");
		
		config1.setNettyDiscoveryProtocolTopics(topics);
		
		List<String> topics2= new ArrayList<String>();
		topics2.add("testtopic11");
		topics2.add("testtopic22");
		
		config2.setNettyDiscoveryProtocolTopics(topics2);
		
		assertFalse(config1.equals(config2));
		
	}
	
	private ZooKeeperTransportConfig getZkTransportConfig(){
		ZooKeeperTransportConfig config = new ZooKeeperTransportConfig();
		config.setCxnWaitInMillis(1000);
		config.setRetrycount(1);
		config.setRetryWaitTimeInMillis(1000);
		ZooKeeperNode node1 = new ZooKeeperNode();
		node1.setHostname("localhost");
		node1.setPort(2181);
		List<ZooKeeperNode> listnodes = new ArrayList<ZooKeeperNode>();
		listnodes.add(node1);
		
		config.setZknodes(listnodes);
		
		return config;
		
	}

}
