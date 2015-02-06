/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.zookeeper.lp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.messaging.transport.zookeeper.GroupMembership;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperConnectionUtil;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;

public class ProducerFactory implements Runnable, ConnectionStateListener, Watcher, BackgroundCallback {

	public static String zknode = "localhost:2181";
	private int num_producers = 2;
	private CuratorFramework[] connections;
	private String groupname;
	private int num_loops = 5;
	private String producername ;
	private int publishinterval;

	public ProducerFactory(String producername, String groupname, String zknode, int numproducers , int numloops, int publishinterval) {
		this.groupname = groupname;
		this.zknode = zknode;
		this.num_producers = numproducers;
		this.num_loops = numloops;
		this.producername = producername;
		this.publishinterval = publishinterval;
	}

	public void createProducers() {
		try {
			System.out.println("Number of Producers to be created :" + num_producers);
			connections = new CuratorFramework[num_producers];
			for (int i = 0; i < num_producers; i++) {
				CuratorFramework curator;
				synchronized (this) {
					curator = ZooKeeperConnectionUtil
							.createZookeeperConnection(zknode, 36000, 1000,
									this, 3, 1000);
					this.wait(2000);
					connections[i] = curator;
					String membername = producername + i + "_member_"+ i;
					GroupMembership group = new GroupMembership(groupname,
								membername, connections[i], this);
					group.join();
					System.out.println("Producer - " + membername  + " Connection created to ZooKeeper");
				}
			}
			 System.out.println(num_producers + " Connections created");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState state) {
		if (state == ConnectionState.LOST) {

		} else if (state == ConnectionState.CONNECTED) {
			System.out.println("Connected to Zookeeper");
			try {
				synchronized (this) {
					this.notifyAll();
				}

			} catch (Throwable t) {
				t.printStackTrace();

			}

		} else if (state == ConnectionState.RECONNECTED) {
			try {
				synchronized (this) {
					this.notifyAll();
				}

			} catch (Throwable t) {
				t.printStackTrace();

			}
		}

	}
	
	private void publishData() {
		for (int i = 0; i < num_producers; i++) {
			String membername = producername + i + "_member_"
					+ i;
			try {
				GroupMembership group = new GroupMembership(groupname,
						membername, connections[i], this);
				group.setMemberData(membername, getData());
				//System.out.println("Data Published for : " + membername);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private byte[] getData() throws IOException {

		List<JetstreamTopic> topics = new ArrayList<JetstreamTopic>();
		topics.add(new JetstreamTopic("Rtbdpod.RTD"));
		topics.add(new JetstreamTopic("Rtbdpod.ER"));
		EventConsumerAdvertisement data = new EventConsumerAdvertisement(8080,
				"hostnmae", "Rtbdpod.Messaging", topics);
		data.setTopic(new JetstreamTopic("Rtbdpod.RTD"));

		ByteArrayOutputStream out_stream = new ByteArrayOutputStream(64000);
		out_stream.reset();
		ObjectOutputStream out = new ObjectOutputStream(out_stream);
		out.writeObject(data);
		out.flush();

		byte buf[] = out_stream.toByteArray();

		return buf;
	}

	@Override
	public void process(WatchedEvent event) {

	}

	@Override
	public void run() {
		int i = 0;
		while (i < num_loops) {
			System.out.println("Publishing Data -" + i );
			publishData();
			i++;
			try {
				Thread.sleep(publishinterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Total Messages set :" + num_loops * num_producers);
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void processResult(CuratorFramework client, CuratorEvent event)
			throws Exception {
		
	}

}
