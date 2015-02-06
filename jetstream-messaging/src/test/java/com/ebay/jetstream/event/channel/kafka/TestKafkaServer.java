/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.apache.commons.io.FileUtils;

import com.google.common.io.Files;

public class TestKafkaServer {

	private KafkaConfig kafkaConfig;
	private KafkaServerStartable kafkaServer;

	private File locate;

	public TestKafkaServer(Properties properties) {
		String path = properties.getProperty("log.dir");
		locate = Files.createTempDir();
		File dir = new File(locate, path).getAbsoluteFile();
		properties.setProperty("log.dir", dir.getAbsolutePath());

		kafkaConfig = new KafkaConfig(properties);
		System.out.println("num.partitions =" + kafkaConfig.numPartitions());
		System.out.println("background.threads ="
				+ kafkaConfig.backgroundThreads());
		kafkaServer = new KafkaServerStartable(kafkaConfig);
		try {
			kafkaServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("embedded kafka is up");
	}

	public TestKafkaServer(String logDir, int port, int brokerId,
			String zkConnect, int numPartitions) {
		this(createProperties(logDir, port, brokerId, zkConnect, numPartitions));
	}

	private static Properties createProperties(String logDir, int port,
			int brokerId, String zkConnect, int numPartitions) {
		Properties properties = new Properties();
		properties.put("port", port + "");
		properties.put("broker.id", brokerId + "");
		properties.put("num.partitions", numPartitions + "");
		properties.put("log.dir", logDir);
		properties.put("enable.zookeeper", "true");
		properties.put("zookeeper.connect", zkConnect);
		return properties;
	}

	public void stop() {
		kafkaServer.shutdown();
		try {
			FileUtils.deleteDirectory(locate);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("embedded kafka stop");
	}

}
