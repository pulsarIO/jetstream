/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.zookeeper.lp;

import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperConnectionUtil;
import com.netflix.curator.framework.CuratorFramework;

public class ConsumerFactory implements  Runnable {

	public String zknode = "localhost:2181";
	private String groupname;
	private int num_nodes;
	private Consumer consumers[];
	private boolean upstatus = true;

	public ConsumerFactory( String groupname, String zknode,
			int nodes) {
		this.groupname = groupname;
		this.zknode = zknode;
		this.num_nodes = nodes;
		consumers = new Consumer[num_nodes];
	}

	public void createConsumers() {
		try {

			consumers = new Consumer[num_nodes];
			System.out.println("Number of Consumers to be Created : " + num_nodes);
			for (int i = 0; i < num_nodes; i++) {
				CuratorFramework curator = null;
				synchronized (this) {
					Consumer consumer = new Consumer();
					curator = ZooKeeperConnectionUtil
							.createZookeeperConnection(zknode, 36000, 1000,
									consumer, 3, 1000);
					this.wait(2000);
					consumer.setConnection(curator);
					consumers[i] = consumer;
					consumer.subscribe(groupname);
					System.out.println("Consumer_" + i  +" created");
				}
			}
			 System.out.println(num_nodes + " Connections created");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	
	public void stopConsuming(){
		upstatus = false;
	}



	@Override
	public void run() {
		
		while(true){
			long nodeCreated = 0;
			long childrenChanged = 0;
			long dataChanged = 0;
			long dataChangeReceived = 0;
			try {
				for(int i =0 ; i < consumers.length ; i++){
					childrenChanged += consumers[i].getChildrenChanged();
					nodeCreated += consumers[i].getNodecreated();
					dataChanged += consumers[i].getDatachanged();
					dataChangeReceived += consumers[i].getDataChangeReceived();
					//System.out.println("Consumer_"+i + " :: " + consumers[i].getDatachanged());
				}
				System.out.println("NodeCreated :" + nodeCreated + ", DataChanged : "
						+ dataChanged + " , ChildrenChanged : " + childrenChanged + " dataChangeReceived : " +  dataChangeReceived);
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		
	}
	
	public static void main(String[] args) {
		
	}

}
