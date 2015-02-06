/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkConnectorTest {

	private static TestZookeeperServer zkServer;
	private static KafkaControllerConfig config;
	private static KafkaController kafkaController;
	private static KafkaController.ZkConnector zkConnector;

	@BeforeClass
	public static void setUp() throws Exception {
		try {
			zkServer = new TestZookeeperServer(30000, 2182, 100);
			zkServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		config = new KafkaControllerConfig();
		config.setRebalanceInterval(0);
		config.setRebalanceableWaitInMs(0);
		config.setZkConnect("localhost:2182");
		kafkaController = new KafkaController();
		kafkaController.setConfig(config);

		kafkaController.init();
		zkConnector = kafkaController.getZkConnector();
	}

	@Test
	public void testCreate() {
		String path = "/testCreate";
		zkConnector.create(path, false);
		assertTrue(zkConnector.exists(path));
		zkConnector.create(path, false);
	}
	
	@Test
	public void testExist() {
		String path = "/testExist";
		assertFalse(zkConnector.exists(path));
		zkConnector.create(path, false);
		assertTrue(zkConnector.exists(path));
	}

	@Test
	public void testGetChildren() {
		String parent = "/parent";
		String child1 = "c1";
		String child2 = "c2";
		String path1 = parent + "/" + child1;
		String path2 = parent + "/" + child2;
		zkConnector.create(path1, false);
		zkConnector.create(path2, false);
		List<String> children = zkConnector.getChildren(parent);
		assertEquals(2, children.size());
		assertTrue(children.contains(child1));
		assertTrue(children.contains(child2));
		
		String parent2 = "/parent2";
		List<String> children2 = zkConnector.getChildren(parent2);
		assertNull(children2);
	}

	@Test
	public void testDelete() {
		String path = "/testDelete";
		assertFalse(zkConnector.exists(path));
		zkConnector.create(path, false);
		assertTrue(zkConnector.exists(path));
		zkConnector.delete(path);
		assertFalse(zkConnector.exists(path));
	}

	@Test
	public void testReadAndWriteString() {
		String path = "/testString";
		String str = "testStringContent";
		zkConnector.writeString(path, str);
		assertTrue(zkConnector.exists(path));
		String data = zkConnector.readString(path);
		assertNotNull(data);
		assertEquals(data, str);
	}

	@Test
	public void testReadAndWriteJSON() {
		 String path = "/testJSON";
		 Map<String, Object> map = new HashMap<String, Object>();
		 map.put("key1", "value1");
		 map.put("key2", 100);
		 zkConnector.writeJSON(path, map);
		 assertTrue(zkConnector.exists(path));
		 Map<String, Object> data = zkConnector.readJSON(path);
		 assertNotNull(data);
		 assertEquals("value1", data.get("key1"));
		 assertEquals(100, data.get("key2"));
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		kafkaController.shutDown();
		zkServer.shutdown();
	}

}
