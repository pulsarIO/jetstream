/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.zookeeper.lp;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;

public class Consumer implements Watcher, ConnectionStateListener, BackgroundCallback {

	private CuratorFramework connection;
		private long nodecreated = 0;
	private long childrenChanged = 0;
	private long datachanged = 0;
	private long dataChangeReceived = 0;
	
	public long getDataChangeReceived() {
		return dataChangeReceived;
	}

	public Consumer(CuratorFramework connection) {
		this.connection = connection;
	}
	
	public Consumer(){
		
	}
	
	public void setConnection(CuratorFramework connection) {
		this.connection = connection;
	}
	

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState state) {
		if (state == ConnectionState.LOST) {
			
			System.out.println("Connection lost:::");

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
			System.out.println("Reconnected to Zookeeper");
			try {
				synchronized (this) {
					this.notifyAll();
				}

			} catch (Throwable t) {
				t.printStackTrace();

			}
		}

	}
	
	public long getNodecreated() {
		return nodecreated;
	}


	public void setNodecreated(long nodecreated) {
		this.nodecreated = nodecreated;
	}


	public long getChildrenChanged() {
		return childrenChanged;
	}


	public void setChildrenChanged(long childrenChanged) {
		this.childrenChanged = childrenChanged;
	}


	public long getDatachanged() {
		return datachanged;
	}


	public void setDatachanged(long datachanged) {
		this.datachanged = datachanged;
	}


	public void subscribe(String path){
	  setChildrenWatch(path);
	}

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case NodeChildrenChanged:
			childrenChanged++;
			setChildrenWatch(event.getPath());
			break;
		case NodeCreated:
			nodecreated++;
			setChildrenWatch(event.getPath());
			break;
		case NodeDataChanged:
			//System.out.println(" changed Node" + event.getPath());
			datachanged++;
			getData(event.getPath());
			setNodeWatch(event.getPath());
			
			break;
		case NodeDeleted:
			break;
		case None:
			break;
		default:
			break;
		}
	}
	
	private void setChildrenWatch(String path) {
		List<String> children = null;
		try {
			children = connection.getChildren().usingWatcher(this)
					.forPath(path);
			//System.out.println("Path : " + path);
			//System.out.println("Number of Children" + children.size());
			if(children.size() > 0){
				//System.out.println("Number of Children" + children.size());
				for (String child : children) {
					String childnodePath = (new StringBuilder()).append(path)
							.append("/").append(child).toString();
	
					//System.out.println("Children  :: " + childnodePath);
					connection.checkExists().usingWatcher(this)
							.forPath(childnodePath);
					setChildrenWatch(childnodePath);
				}
			} else {
				connection.checkExists().usingWatcher(this).forPath(path);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void setNodeWatch(String path){
		try {
			connection.checkExists().usingWatcher(this).forPath(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private void getData(String path){
		try {
			 connection.getData().usingWatcher(this).inBackground(this).forPath(path);
			 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void processResult(CuratorFramework client, CuratorEvent event)
			throws Exception {
		switch(event.getType()){
			case CHILDREN:
				break;
			case GET_DATA:
					dataChangeReceived++;
				break;
			case EXISTS:
				break;
				
		}
		
	}

}
