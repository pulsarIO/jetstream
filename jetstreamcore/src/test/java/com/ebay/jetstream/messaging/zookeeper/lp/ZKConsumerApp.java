/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.zookeeper.lp;

import java.util.concurrent.atomic.AtomicBoolean;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.RootConfiguration;

public class ZKConsumerApp {
	
	String groupname = "/ZKTesting.run";
	private String zknode ="localhost:2181";
	int num_consumers = 10;
	String producername = "producer";
	
	public static AtomicBoolean producerstatus = new AtomicBoolean(true);

	public String getProducername() {
		return producername;
	}

	public void setProducername(String producername) {
		this.producername = producername;
	}
	
	public String getZknode() {
		return zknode;
	}

	public void setZknode(String zknode) {
		this.zknode = zknode;
	}

	
	
	public String getGroupname() {
		return groupname;
	}

	public void setGroupname(String groupname) {
		this.groupname = groupname;
	}

	public int getNum_consumers() {
		return num_consumers;
	}

	public void setNum_consumers(int num_consumers) {
		this.num_consumers = num_consumers;
	}

	public static void main(String[] args) throws InterruptedException {
		
		ZKConsumerApp tester = new ZKConsumerApp();
		
		String filename = null;
		if(args != null && args.length >= 1)
			 filename = args[0];
		else
			filename ="src/test/java/com/ebay/jetstream/messaging/zookeeper/lp/ZKConsumerConfig.xml" ;
		
		System.out.println("Config File : " + filename);
		RootConfiguration rc =  new RootConfiguration(new ApplicationInformation("ZKProdConsumer", "0.0.5.0"),
			        new String[] { filename });
		
		tester= (ZKConsumerApp) rc.getBean("ZkTest");
		
		//int num_consumers = 10;
		//int num_producers = 10;
		//int num_runs = 5;
		
		ConsumerFactory consumerFactory = new ConsumerFactory(tester.groupname, tester.zknode, tester.getNum_consumers());
		
		consumerFactory.createConsumers();
		
		Thread consumerThread = new Thread(consumerFactory,"Consumer");
		consumerThread.start();
	
		consumerThread.join();
		
	}
	

}
