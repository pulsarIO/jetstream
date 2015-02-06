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

import com.ebay.jetstream.config.ApplicationInformation;
import com.ebay.jetstream.config.RootConfiguration;
import com.ebay.jetstream.messaging.topic.JetstreamTopic;
import com.ebay.jetstream.messaging.transport.netty.protocol.EventConsumerAdvertisement;
import com.ebay.jetstream.messaging.transport.zookeeper.GroupMembership;
import com.ebay.jetstream.messaging.transport.zookeeper.ZooKeeperConnectionUtil;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;

public class ZKTester {

	private int num_threads = 50;
	private int total_nodes = 5000;
	String groupname = "/ZKTesting.run";
	long sendinterval = 10000;
	long runtime =  60 *60 * 1000 ;
	public static String zknode ="loclahost:2181";
	
	public static String getZknode() {
		return zknode; 
	}

	public static void setZknode(String zknode) {
		ZKTester.zknode = zknode;
	}

	public void setGroupname(String groupname) {
		this.groupname = groupname;
	}

	public void setSendinterval(long sendinterval) {
		this.sendinterval = sendinterval;
	}

	public long getRuntime() {
		return runtime;
	}

	public void setRuntime(long runtime) {
		this.runtime = runtime;
	}

	public String getGroupname() {
		return groupname;
	}

	public long getSendinterval() {
		return sendinterval;
	}

	public int getTotal_nodes() {
		return total_nodes;
	}

	public void setTotal_nodes(int total_nodes) {
		this.total_nodes = total_nodes;
	}

	public int getNum_threads() {
		return num_threads;
	}

	public void setNum_threads(int num_threads) {
		this.num_threads = num_threads;
	}

	public static void main(String[] args) throws InterruptedException {
		String filename = null;
		if(args != null && args.length >= 1)
			 filename = args[0];
		else
			filename ="src/test/java/com/ebay/jetstream/messaging/zookeeper/lp/ZKTestContext.xml" ;
		
		System.out.println("Config File : " + filename);
		RootConfiguration rc =  new RootConfiguration(new ApplicationInformation("Zktester", "0.0.5.0"),
			        new String[] { filename });
		
		ZKTester tester= (ZKTester) rc.getBean("ZkTest");
		int nummem = tester.total_nodes / tester.num_threads;
	//	tester.runtime = 10 * 60 *60 * 1000;

		System.out.println("ZooKeeper Server : " + zknode);
		System.out.println("Total number of Threads : " + tester.num_threads);
		System.out
				.println("Total number of nodes to be created in ZooKeeper : "
						+ tester.total_nodes);

		Thread[] testrunners = new Thread[tester.num_threads];
		
		for (int i = 0; i < tester.num_threads; i++) {
			String tname = "ZKRunner_" + i;

			ZKRunner runner = new ZKRunner(tester.groupname, nummem,
					tester.sendinterval, tester.runtime);
			runner.setName(tname);

			testrunners[i] = runner;
			testrunners[i].start();

		}

		for (int i = 0; i < testrunners.length; i++) {
			testrunners[i].join();
		}

		System.out.println("\n Test Completed....");

	}

}

class ZKRunner extends Thread implements Watcher, ConnectionStateListener {

	String groupname;
	private int num_members;
	private long sendinterval;
	private CuratorFramework framework;
	private long sentcounter = 0;
	private long listenercounter = 0;
	private long runtime = 5000;
	private CuratorFramework[] connections;

	ZKRunner(String groupname, int membercount, long sendinterval, long runtime) {
		this.groupname = groupname;
		this.num_members = membercount;
		this.sendinterval = sendinterval;
		this.runtime = runtime;
	}

	@Override
	public void run() {

		try {

			System.out.println("Starting..." + Thread.currentThread().getName());
			connections = new CuratorFramework[num_members];
			createConnections();
			System.out.println("Conenctions Created..." + Thread.currentThread().getName());
			joingroup();

			while (runtime > 0) {
				
				try{
					senddata();
					Thread.sleep(sendinterval);
					runtime = runtime - sendinterval;
				} catch(Exception e){
					e.printStackTrace();
				}
			}
			// lets sleep for few seconds to receive all notifications
			Thread.sleep(2000);

			System.out.println("\n Thread : " + getName()
					+ " ,  Number of sent : " + sentcounter
					+ " , Number of listener received : " + listenercounter);
			// Assert.assertEquals(sentcounter, listenercounter);
			close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void close() {
		for (int i = 0; i < num_members; i++) {
			connections[i].close();
		}
	}

	private void createConnections() {
		try {
			
			for (int i = 0; i < num_members; i++) {
				CuratorFramework curator;
					synchronized (this) {
					 	 curator = ZooKeeperConnectionUtil
						.createZookeeperConnection(ZKTester.zknode, 36000,
								1000, this, 3, 1000);
						this.wait(10000);
					}
					connections[i] = curator;
					
					
			}
			System.out.println(num_members + "Connectiosn created");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void getConnection() {
		try {
			framework = ZooKeeperConnectionUtil.createZookeeperConnection(
					"127.0.0.1:2181", 36000, 1000, null, 3, 1000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void senddata() throws Exception {

		System.out.println(Thread.currentThread().getName()
				+ " : Setting data to nodes..");
		for (int i = 0; i < num_members; i++) {

			String membername = Thread.currentThread().getName() + "_member_"
					+ i;
			GroupMembership group = new GroupMembership(groupname, membername,
					connections[i], this);
			group.setMemberData(membername, getData());

			sentcounter++;

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

	private void joingroup() throws Exception {
		/*System.out.println(Thread.currentThread().getName()
				+ " Creating nodes..");*/
		for (int i = 0; i < num_members; i++) {

			String membername = Thread.currentThread().getName() + "_member_"
					+ i;
			GroupMembership group = new GroupMembership(groupname, membername,
					connections[i], this);
			group.join();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		

		switch (event.getType()) {

		case NodeChildrenChanged:
			break;
		case NodeCreated:
			break;
		case NodeDataChanged:
			try {
			//	Stat stat = framework.checkExists().forPath(event.getPath());
			//	long curtime = System.currentTimeMillis();
			//	long delay = curtime - stat.getMtime();
				// System.out.println("Notification delay : " + delay);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			listenercounter++;

			break;
		case NodeDeleted:
			break;
		case None:
			break;
		default:
			break;
		}

	}

	@Override
	public void stateChanged(CuratorFramework arg0, ConnectionState state) {
		if (state == ConnectionState.LOST) {
			
		} else if (state == ConnectionState.CONNECTED) {
			System.out.println("Connected");
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

}
