/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.ebay.jetstream.config.ContextBeanChangedEvent;

public class KafkaControllerTest {

	private static TestZookeeperServer zkServer;
	private static KafkaControllerConfig config;
	private static String configBeanName = "kafkaControllerConfig";
	private static KafkaController kafkaController;
	private static KafkaController.ZkConnector zkConnector;

	@BeforeClass
	public static void setUp() throws Exception {
		try {
			zkServer = new TestZookeeperServer(30000, 2183, 100);
			zkServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		config = new KafkaControllerConfig();
		config.setBeanName(configBeanName);
		config.setRebalanceInterval(3000);
		config.setRebalanceableWaitInMs(0);
		config.setZkConnect("localhost:2183");
		
		kafkaController = new KafkaController();
		kafkaController.setConfig(config);

		kafkaController.init();
		zkConnector = kafkaController.getZkConnector();
	}

	@Test
	public void testRegisterAndUnregister() throws Exception {
		KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
		when(kafkaConsumer.calcRebalance()).thenReturn(new HashMap<String, Integer>());
		when(kafkaConsumer.takePartitions(anyString(), anyInt())).thenReturn(true);
		when(kafkaConsumer.releasePartitions(anyString(), anyInt())).thenReturn(true);
		
		String beanName = "testConsumer1";
		kafkaController.register(beanName, kafkaConsumer);
		Thread.sleep(3500);
		
		verify(kafkaConsumer).coordinate();
		verify(kafkaConsumer).calcRebalance();
		
		kafkaController.unregister(beanName);
		Thread.sleep(7000);
		
		verify(kafkaConsumer, atMost(1)).coordinate();
		verify(kafkaConsumer, atMost(1)).calcRebalance();
	}

	@Ignore
	@Test
	public void testConnectionLossAndBack() throws Exception {
		zkServer.serverShutdown();
		Thread.sleep(5000);
		assertFalse(zkConnector.isZkConnected());
		assertTrue(kafkaController.isStarted());
		assertFalse(kafkaController.isRebalanceable());

		zkServer.serverStartup();
		Thread.sleep(5000);
		assertTrue(zkConnector.isZkConnected());
		assertTrue(kafkaController.isRebalanceable());
	}
	
	@Test
	public void testHotDeploy() throws Exception {
		KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
		kafkaController.register("consumer1", kafkaConsumer);
		
		KafkaControllerConfig newConfig = new KafkaControllerConfig();
		newConfig.setBeanName(configBeanName);
		newConfig.setRebalanceInterval(3000);
		newConfig.setRebalanceableWaitInMs(0);
		newConfig.setZkConnect("localhost:2183");
		
		ContextBeanChangedEvent bcInfo = mock(ContextBeanChangedEvent.class);
		when(bcInfo.getBeanName()).thenReturn(configBeanName);
		when(bcInfo.getChangedBean()).thenReturn(newConfig);
		when(bcInfo.isChangedBean(config)).thenReturn(true);
		
		kafkaController.onApplicationEvent(bcInfo);
		
		Thread.sleep(4000);
		
		assertEquals(newConfig, kafkaController.getConfig());
		
		verify(kafkaConsumer).stop();
		verify(kafkaConsumer).start();
		
		assertTrue(zkConnector.isZkConnected());
		assertTrue(kafkaController.isStarted());
		assertTrue(kafkaController.isRebalanceable());
	}

	@AfterClass
	public static void tearDown() throws Exception {
		kafkaController.shutDown();
		zkServer.shutdown();
	}

}
