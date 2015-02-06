/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.zookeeper.lp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.RootConfiguration;

public class ZKProducerApp {
	
	String groupname = "/zktester.run";
	private String zknode ="localhost:2181";
	int num_producers = 10;
	int num_runs = 1000;
	String producername = "producer";
	private int publishintervalinms = 2000 ;
	
	public static AtomicBoolean producerstatus = new AtomicBoolean(true);

	public int getPublishintervalinms() {
		return publishintervalinms;
	}

	public void setPublishintervalinms(int publishintervalinms) {
		this.publishintervalinms = publishintervalinms;
	}

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

	
	public int getNum_runs() {
		return num_runs;
	}

	public void setNum_runs(int num_runs) {
		this.num_runs = num_runs;
	}
	
	public String getGroupname() {
		return groupname;
	}

	public void setGroupname(String groupname) {
		this.groupname = groupname;
	}

	public int getNum_producers() {
		return num_producers;
	}

	public void setNum_producers(int num_producers) {
		this.num_producers = num_producers;
	}

	
	public static void main(String[] args) throws InterruptedException, IOException {
	
		String filename = null;
		if(args != null && args.length >= 1)
			 filename = args[0];
		else
			filename ="src/test/java/com/ebay/jetstream/messaging/zookeeper/lp/ZKProducerConfig.xml" ;
		
		System.out.println("Config File : " + filename);
		
		RootConfiguration rc =  new RootConfiguration(new ApplicationInformation("ZKProdConsumer", "0.0.5.0"),
			        new String[] { filename });
		
		ZKProducerApp tester= (ZKProducerApp) rc.getBean("ZkTest");
		
		ProducerFactory producerFactory = new ProducerFactory(tester.getProducername(), tester.groupname, tester.zknode, tester.getNum_producers(), tester.getNum_runs(), tester.getPublishintervalinms());
		
		producerFactory.createProducers();
		
		Thread producerThread = new Thread(producerFactory,"Producer");
		
		System.out.println("Press Enter to start publishing data....");
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String input = reader.readLine();
		producerThread.start();
		
		producerThread.join();
		System.out.println("Completed Publishing data....");
		
	}
	

}
