/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaControllerInitTest {

	private TestZookeeperServer zkServer;
	private KafkaControllerConfig config;
	private KafkaController kafkaController;

	@Before
	public void setUp() {
		try {
			zkServer = new TestZookeeperServer(30000, 2183, 100);
			zkServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		config = new KafkaControllerConfig();
		config.setRebalanceInterval(0);
		config.setRebalanceableWaitInMs(0);
	}

	@Test
	public void testInit() throws Exception {
		config.setZkConnect("localhost:2183");
		kafkaController = new KafkaController();
		kafkaController.setConfig(config);
		kafkaController.init();
		KafkaController.ZkConnector zkConnector = kafkaController
				.getZkConnector();
		assertNotNull(zkConnector);
		assertTrue(kafkaController.isStarted());
		//assertTrue(zkConnector.isZkConnected());
	}

	@Test(expected = RuntimeException.class)
	public void testInitFail() {
		config.setZkConnect("localhost:2184");
		kafkaController = new KafkaController();
		kafkaController.setConfig(config);
		kafkaController.init();
	}

	@After
	public void tearDown() throws Exception {
		zkServer.shutdown();
	}

}
