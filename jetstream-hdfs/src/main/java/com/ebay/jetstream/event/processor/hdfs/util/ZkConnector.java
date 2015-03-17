package com.ebay.jetstream.event.processor.hdfs.util;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryNTimes;

/**
 * @author xiaojuwu1
 * 
 */
public class ZkConnector {

	private static final Logger LOGGER = Logger.getLogger(ZkConnector.class
			.getName());

	private CuratorFramework curator;

	private AtomicBoolean zkConnected = new AtomicBoolean(false);

	public ZkConnector(String zkConnect, int zkConnectionTimeoutMs,
			int zkSessionTimeoutMs, int retryTimes, int sleepMsBetweenRetries) {
		curator = CuratorFrameworkFactory.newClient(zkConnect,
				zkSessionTimeoutMs, zkConnectionTimeoutMs, new RetryNTimes(
						retryTimes, sleepMsBetweenRetries));
		curator.start();
		curator.getConnectionStateListenable().addListener(
				createConnectionStateListener());
		boolean success = false;
		try {
			success = curator.getZookeeperClient()
					.blockUntilConnectedOrTimedOut();
		} catch (InterruptedException e) {
		}
		if (!success)
			throw new RuntimeException(
					"Fail to establish zookeeper connection.");
		else
			LOGGER.log(Level.INFO, "Zookeeper connection is established.");
	}

	private ConnectionStateListener createConnectionStateListener() {
		return new ConnectionStateListener() {
			@Override
			public void stateChanged(CuratorFramework client,
					ConnectionState newState) {
				LOGGER.log(Level.INFO,
						"Curator Connection state is changed to " + newState);
				if (newState == ConnectionState.CONNECTED) {
					zkConnected.set(true);
				} else if (newState == ConnectionState.SUSPENDED
						|| newState == ConnectionState.LOST) {
					zkConnected.set(false);
				} else if (newState == ConnectionState.RECONNECTED) {
					zkConnected.set(true);
				}
			}
		};
	}

	/** for zookeeper functions **/
	public boolean exists(String path) {
		try {
			return curator.checkExists().forPath(path) != null;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void create(String path, boolean ephemeral) {
		try {
			if (exists(path))
				return;
			CreateMode createMode = ephemeral ? CreateMode.EPHEMERAL
					: CreateMode.PERSISTENT;
			curator.create().creatingParentsIfNeeded().withMode(createMode)
					.forPath(path);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public List<String> getChildren(String path) {
		try {
			if (!exists(path))
				return null;
			return curator.getChildren().forPath(path);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String readString(String path) {
		try {
			byte[] data = readBytes(path);
			if (data == null)
				return null;
			return new String(data, "UTF-8");
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Map<String, Object> readJSON(String path) {
		try {
			byte[] data = readBytes(path);
			if (data == null)
				return null;
			ObjectMapper jsonMapper = new ObjectMapper();
			TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>() {
			};
			return jsonMapper.readValue(data, 0, data.length, type);
		} catch (JsonMappingException e) {
			return null;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void writeString(String path, String data) {
		try {
			writeBytes(path, data.getBytes(Charset.forName("UTF-8")));
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void writeJSON(String path, Map<String, Object> data) {
		try {
			ObjectMapper jsonMapper = new ObjectMapper();
			byte[] bytes = jsonMapper.writeValueAsBytes(data);
			writeBytes(path, bytes);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] readBytes(String path) {
		try {
			if (!exists(path))
				return null;
			return curator.getData().forPath(path);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void writeBytes(String path, byte[] bytes) {
		try {
			if (!exists(path))
				create(path, false);
			curator.setData().forPath(path, bytes);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void delete(String path) {
		try {
			if (!exists(path))
				return;
			curator.delete().forPath(path);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		curator.close();
	}

	public boolean isConnected() {
		return zkConnected.get();
	}

}
