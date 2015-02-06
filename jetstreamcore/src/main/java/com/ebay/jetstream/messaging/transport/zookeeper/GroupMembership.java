/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;


public class GroupMembership {

	private String groupname;
	private String membername;
	private Watcher watcher;
	private final static String FWD_SLASH = "/";
	private ZooKeeperTransport transport; 
	private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging");

	public GroupMembership(String groupname, String membername,
			CuratorFramework client, Watcher watcher) throws Exception {
		this.groupname = groupname;
		this.membername = membername;
		//this.transport.getZKHandle() = client;
		this.watcher = watcher;
		creategroup();
	}
	
	public GroupMembership(String groupname, String membername,
			ZooKeeperTransport transport, Watcher watcher) throws Exception {
		this.groupname = groupname;
		this.membername = membername;
		//this.transport.getZKHandle() = transport.getZKHandle();
		this.transport = transport;
		this.watcher = watcher;
		creategroup();
	}
	
	public String getGroupname() {
		return groupname;
	}
	

	@SuppressWarnings("static-access")
	private void creategroup() throws Exception {
		
		if (transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(groupname) == null) {
			
			transport.getZKHandle().getZookeeperClient()
					.newRetryLoop()
					.callWithRetry(transport.getZKHandle().getZookeeperClient(),
							new Callable<String>() {
								public String call() throws Exception {
									return transport.getZKHandle().create()
											.withMode(CreateMode.PERSISTENT)
											.forPath(groupname);
								}
							});
		}
	}
	
	@SuppressWarnings("static-access")
	public void createsubgroups(String path) throws Exception {

		if (transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(groupname) != null) {
			final String subgrouppath;

			if (!path.contains(groupname)) {
				subgrouppath = path.contains(FWD_SLASH) ? new StringBuffer(
						groupname).append(path).toString() : new StringBuffer(
						groupname).append(FWD_SLASH).append(path).toString();
			} else {
				subgrouppath = path;
			}

			// make sure the subgroup is not exists
			if (transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(subgrouppath) == null) {
				transport.getZKHandle().getZookeeperClient()
						.newRetryLoop()
						.callWithRetry(transport.getZKHandle().getZookeeperClient(),
								new Callable<String>() {
									public String call() throws Exception {
										return transport.getZKHandle()
												.create()
												.creatingParentsIfNeeded()
												.withMode(CreateMode.PERSISTENT)
												.forPath(subgrouppath);
									}
								});
			}
		} else {
			throw new Exception("Group " + groupname
					+ " should be created before creating subgroup - " + path);
		}
	}
	

	@SuppressWarnings("static-access")
	public void setMemberData(final String membername, final byte[] data)
			throws Exception {
		
		final String memberpath = new StringBuffer(groupname).append(FWD_SLASH)
		.append(membername).toString();
		
		if (transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(memberpath) == null) {
			join(groupname);
		}

		transport.getZKHandle().getZookeeperClient().newRetryLoop()
				.callWithRetry(transport.getZKHandle().getZookeeperClient(), new Callable<Stat>() {
					public Stat call() throws Exception {
						return transport.getZKHandle().setData().forPath(memberpath, data);
			}
		});

	}

	@SuppressWarnings("static-access")
	public void setGroupMemberData(final String groupname, final byte[] data)
			throws Exception {
		final String memberpath = new StringBuffer(groupname).append(FWD_SLASH)
				.append(membername).toString();
	
		if (transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(memberpath) == null) {
			join(groupname);
		}
		transport.getZKHandle().getZookeeperClient().newRetryLoop()
				.callWithRetry(transport.getZKHandle().getZookeeperClient(), new Callable<Stat>() {
					public Stat call() throws Exception {
						return transport.getZKHandle().setData().forPath(memberpath, data);
					}
				});

	}

	@SuppressWarnings("static-access")
	public void setGroupData(final String group, final byte[] data)
			throws Exception {

		transport.getZKHandle().getZookeeperClient().newRetryLoop()
				.callWithRetry(transport.getZKHandle().getZookeeperClient(), new Callable<Stat>() {
					public Stat call() throws Exception {
						return transport.getZKHandle().setData().forPath(group, data);
					}
				});

	}

	/**
	 * Member can join the group as ephemeral node.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("static-access")
	public void join() throws Exception {

		final String memberpath = new StringBuffer(groupname).append(FWD_SLASH)
				.append(membername).toString();

		if (transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(memberpath) == null) {
			transport.getZKHandle().getZookeeperClient()
					.newRetryLoop()
					.callWithRetry(transport.getZKHandle().getZookeeperClient(),
							new Callable<String>() {
								public String call() throws Exception {
									return transport.getZKHandle().create()
											.creatingParentsIfNeeded()
											.withMode(CreateMode.EPHEMERAL)
											.forPath(memberpath);
								}
							});
		}
	}

	@SuppressWarnings("static-access")
	public void join(final String groupname) throws Exception {
		create(groupname);
		final String memberpath = new StringBuffer(groupname).append(FWD_SLASH)
				.append(membername).toString();

		if (transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(memberpath) == null) {
			transport.getZKHandle().getZookeeperClient()
					.newRetryLoop()
					.callWithRetry(transport.getZKHandle().getZookeeperClient(),
							new Callable<String>() {
								public String call() throws Exception {

									return transport.getZKHandle().create()
											.creatingParentsIfNeeded()
											.withMode(CreateMode.EPHEMERAL)
											.forPath(memberpath);
								}
							});
		}

		setChildrenWatch(groupname);

	}

	@SuppressWarnings("static-access")
	public void setChildrenWatch(final String path) {
		
		try {
			transport.getZKHandle().getZookeeperClient()
					.newRetryLoop()
					.callWithRetry(transport.getZKHandle().getZookeeperClient(),
							new Callable<List<String>>() {
								public List<String> call() {
									try {
										return transport.getZKHandle().getChildren()
												.inBackground((BackgroundCallback)watcher)
												.forPath(path);
									} catch (Exception e) {
										LOGGER.error( e.getMessage(), e);
									}
									return null;
								}
							});
		} catch (Exception e) {
			LOGGER.error(e.getMessage() , e);
		}
	}

	/**
	 * Member can join the subgroup. if subgroup not exists, it will be created
	 * based
	 * 
	 * @param subgroup
	 */
	@SuppressWarnings("static-access")
	private void create(final String subgroup) throws Exception {

		if (transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(subgroup) == null) {
			transport.getZKHandle().getZookeeperClient()
					.newRetryLoop()
					.callWithRetry(transport.getZKHandle().getZookeeperClient(),
							new Callable<String>() {
								public String call() throws Exception {
									return transport.getZKHandle().create()
											.creatingParentsIfNeeded()
											.withMode(CreateMode.PERSISTENT)
											.forPath(subgroup);
								}
							});
		}
	}

	/**
	 * Member can leave the group.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("static-access")
	public void leave() throws Exception {
		transport.getZKHandle().getZookeeperClient().newRetryLoop()
				.callWithRetry(transport.getZKHandle().getZookeeperClient(), new Callable<Void>() {
					public Void call() throws Exception {
						String memberpath = new StringBuffer(groupname)
								.append(FWD_SLASH).append(membername)
								.toString();
						transport.getZKHandle().delete().forPath(memberpath);
						return null;
					}
				});
	}

	/**
	 * Member can leave from given group.
	 */
	@SuppressWarnings("static-access")
	public void leave(final String subgroup) throws Exception {

		transport.getZKHandle().getZookeeperClient().newRetryLoop()
				.callWithRetry(transport.getZKHandle().getZookeeperClient(), new Callable<Void>() {
					public Void call() throws Exception {

						if (subgroup!= null && subgroup.contains(groupname)) {
							String grouptoLeave = new StringBuffer(subgroup)
									.append(FWD_SLASH).append(membername)
									.toString();
							transport.getZKHandle().delete().forPath(grouptoLeave);
						} else {
							leave();
						}

						return null;
					}
				});
	}
	
	/**
	 * Set watch on each child node to receive notifications on node data
	 * change/creation/deletion notifications
	 * 
	 * @param path
	 */
	@SuppressWarnings("restriction")
	public void getChildrenAndSetWatch(String path) {
		List<String> children = null;
		try {
			children = transport.getZKHandle().getChildren().usingWatcher(watcher)
					.forPath(path);
			
			for (String child : children) {
				String childnodePath = (new StringBuilder()).append(path)
						.append("/").append(child).toString();

				transport.getZKHandle().checkExists().usingWatcher(watcher)
						.forPath(childnodePath);
				getChildrenAndSetWatch(childnodePath);
			}
		}catch(NoNodeException nee){
			LOGGER.warn( " NoNodeException While setting children watch for " + path 
					+ nee.getMessage());
		}
		catch (Exception e) {
			LOGGER.error( " Exception While setting watch on "
					+ path, e);
		}
	}
	
	/**
	 * Register callback notifications when child node added/deleted for a znode
	 * 
	 * @param path
	 */
	@SuppressWarnings("restriction")
	public void registerChildrenCallback(String path) {
		try {
			transport.getZKHandle().getChildren().usingWatcher(watcher).inBackground((BackgroundCallback)watcher).forPath(path);
		} catch (Exception e) {
			LOGGER.error(
					"Exception while registering childrencallback for " + path,
					e);
		}
	}
	
	
	public byte[] getMemberData(String path){
		try{
			if(transport.getZKHandle().checkExists().usingWatcher(watcher).forPath(path) != null){
				byte[] data = transport.getZKHandle().getData().usingWatcher(watcher).forPath(path);	
				return data;
			}	
		}catch(Exception e){
			LOGGER.error( "Exception while fetching data for "
					+ path, e);
		}
		return null;
	}

	/**
	 * Register Data call back for the path
	 * 
	 * @param path
	 */
	@SuppressWarnings("restriction")
	public void registerDataCallBack(String path) {
		try {
			transport.getZKHandle().getData().usingWatcher(watcher).inBackground((BackgroundCallback) watcher)
					.forPath(path);
		} catch (Exception e) {
			LOGGER.error( "Exception in setting data call back for path :" + path , e);
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((groupname == null) ? 0 : groupname.hashCode());
		result = prime * result
				+ ((membername == null) ? 0 : membername.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GroupMembership other = (GroupMembership) obj;
		if (groupname == null) {
			if (other.groupname != null)
				return false;
		} else if (!groupname.equals(other.groupname))
			return false;
		if (membername == null) {
			if (other.membername != null)
				return false;
		} else if (!membername.equals(other.membername))
			return false;
		return true;
	}

}
