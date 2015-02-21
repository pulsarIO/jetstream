/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.kafka;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.config.ContextBeanChangedEvent;
import com.ebay.jetstream.management.Management;
import com.ebay.jetstream.spring.beans.factory.BeanChangeAware;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.RetryNTimes;

public class KafkaController extends AbstractNamedBean implements
		InitializingBean, ShutDownable, ApplicationListener, BeanChangeAware {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaController.class
			.getName());

	public class ZkConnector {
		private CuratorFramework m_curator;
		private AtomicBoolean m_zkConnected = new AtomicBoolean(false);

		private List<String> m_nonresolvablehosts = new ArrayList<String>();
		private List<String> m_resolvablehosts = new ArrayList<String>();

		private void createCurator() {
			String resolvablcnxnstr =  findDNSResolvableZkHosts(m_config.getZkConnect());
			m_curator = CuratorFrameworkFactory.newClient(
					resolvablcnxnstr,
					m_config.getZkSessionTimeoutMs(),
					m_config.getZkConnectionTimeoutMs(),
					new RetryNTimes(m_config.getRetryTimes(), m_config
							.getSleepMsBetweenRetries()));
			m_curator.getConnectionStateListenable().addListener(
					createConnectionStateListener());
			m_curator.start();
			boolean success = false;
			try {
				success = m_curator.getZookeeperClient()
						.blockUntilConnectedOrTimedOut();
			} catch (InterruptedException e) {
			}
			if (!success)
				throw new RuntimeException(
						"Fail to establish zookeeper connection.");
			else {
				if (LOGGER.isInfoEnabled())
					LOGGER.info(
							"Zookeeper connection is established.");
			}
		}

		private ConnectionStateListener createConnectionStateListener() {
			return new ConnectionStateListener() {
				@Override
				public void stateChanged(CuratorFramework client,
						ConnectionState newState) {
					if (LOGGER.isInfoEnabled())
						LOGGER.info(
								"Curator Connection state is changed to "
										+ newState);

					if (newState == ConnectionState.CONNECTED) {
						m_zkConnected.set(true);
						if (LOGGER.isInfoEnabled())
							LOGGER.info(
									"Zookeeper connected is set to true.");
						new Thread(new OnConnectedTask()).start();

					} else if (newState == ConnectionState.SUSPENDED
							|| newState == ConnectionState.LOST) {
						m_zkConnected.set(false);
						m_rebalanceable.set(false);
						if (LOGGER.isInfoEnabled())
							LOGGER.info(
									"Zookeeper connection is lost and it's now unrebalanceable.");

					} else if (newState == ConnectionState.RECONNECTED) {
						noticeZkReconnected();
						m_zkConnected.set(true);
						m_rebalanceable.set(true);
						if (LOGGER.isInfoEnabled())
							LOGGER.info(
									"Zookeeper is reconnected and it's rebalanceable again.");
					}
				}
			};
		}
		
		private String findDNSResolvableZkHosts(String origCnxnStr) {
			String[] hostporttuple = origCnxnStr.split(",");
			StringBuffer newcnxStr = new StringBuffer();
			int count = 1;
			//reset resolvable list
			m_resolvablehosts.clear();
			m_nonresolvablehosts.clear();
			for(String hostport : hostporttuple){
				String host = hostport.split(":")[0];
				String port = hostport.split(":")[1];
				try {
					InetAddress.getAllByName(host);
					if(count != 1)
						newcnxStr.append(",");
					
					newcnxStr.append(host);
					newcnxStr.append(":");
					newcnxStr.append(port);
					m_resolvablehosts.add(host);
					count++;
				} catch (UnknownHostException ukhe) {
					String trace = ExceptionUtils.getStackTrace(ukhe);
					m_nonresolvablehosts.add(host);
					LOGGER.error(" Non Resolvable HostName : " +  host + " : " + trace);
				}catch (Throwable t) {
					String trace = ExceptionUtils.getStackTrace(t);
					m_nonresolvablehosts.add(host);
					LOGGER.error(" Got other exception while resolving hostname : " +  host + " : " + trace);
				}
				
			}
			return newcnxStr.toString();
		}

		private synchronized void closeCurator() {
			m_curator.close();
			if (LOGGER.isInfoEnabled())
				LOGGER.info(
						"CuratorFramework is closed for ZkConnector.");
		}

		public boolean isZkConnected() {
			return m_zkConnected.get();
		}

		public boolean exists(String path) {
			try {
				return m_curator.checkExists().forPath(path) != null;
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		public boolean create(String path, boolean ephemeral) {
			try {
				if (exists(path))
					return false;
				CreateMode createMode = ephemeral ? CreateMode.EPHEMERAL
						: CreateMode.PERSISTENT;
				m_curator.create().creatingParentsIfNeeded()
						.withMode(createMode).forPath(path);
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return true;
		}

		public List<String> getChildren(String path) {
			try {
				if (!exists(path))
					return null;
				return m_curator.getChildren().forPath(path);
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

		public Map<String, Object> readJSON(String path) {
			try {
				byte[] data = readBytes(path);
				if (data == null)
					return null;
				ObjectMapper jsonMapper = new ObjectMapper();
				TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>() {
				};
				return jsonMapper.readValue(data, 0, data.length, type);
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

		public byte[] readBytes(String path) {
			try {
				if (!exists(path))
					return null;
				return m_curator.getData().forPath(path);
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
				m_curator.setData().forPath(path, bytes);
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
				m_curator.delete().forPath(path);
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private KafkaControllerConfig m_config;
	private ZkConnector zkConnector;
	private Map<String, KafkaConsumer> m_kafkaConsumers = new ConcurrentHashMap<String, KafkaConsumer>();
	private Timer m_timer = new Timer();

	private AtomicBoolean m_rebalanceable = new AtomicBoolean(false);

	/** pause or started **/
	private AtomicBoolean m_started = new AtomicBoolean(false);
	private Object lock = new Object();

	public void setConfig(KafkaControllerConfig config) {
		this.m_config = config;
	}

	public KafkaControllerConfig getConfig() {
		return m_config;
	}

	public Map<String, KafkaConsumer> getKafkaConsumers() {
		return m_kafkaConsumers;
	}

	public ZkConnector getZkConnector() {
		return zkConnector;
	}
	
	public List<String> getNonresolvablehosts() {
		return zkConnector.m_nonresolvablehosts;
	}
	
	public List<String> getResolvablehosts() {
		return zkConnector.m_resolvablehosts;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Management.addBean(getBeanName(), this);
		init();
	}

	public void init() {
		zkConnector = new ZkConnector();
		zkConnector.createCurator();
		startTimer();
		m_started.set(true);

		if (LOGGER.isInfoEnabled())
			LOGGER.info( "KafkaController is started.");
	}

	/** life cycle control **/

	@Override
	public void shutDown() {
		stop();
		zkConnector.closeCurator();
		if (LOGGER.isInfoEnabled())
			LOGGER.info( "KafkaController is shutdown.");
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		processApplicationEvent(event);
	}

	protected void processApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextBeanChangedEvent) {
			ContextBeanChangedEvent bcInfo = (ContextBeanChangedEvent) event;
			if (bcInfo.isChangedBean(m_config)) {
				setConfig((KafkaControllerConfig) bcInfo.getChangedBean());
				restart();
			}
		}
	}

	public void stop() {
		synchronized (lock) {
			stopAllConsumers();
			cancelTimer();
			zkConnector.closeCurator();
			m_started.set(false);
			if (LOGGER.isInfoEnabled())
				LOGGER.info( "KafkaController is stopped.");
		}
	}

	public void restart() {
		stop();
		synchronized (lock) {
			zkConnector.createCurator();
			startTimer();
			m_started.set(true);
			startAllConsumers();
			if (LOGGER.isInfoEnabled())
				LOGGER.info( "KafkaController is started.");
		}
	}

	public boolean isStarted() {
		return m_started.get();
	}

	private void startTimer() {
		if (m_config.getRebalanceInterval() <= 0)
			return;
		RebalanceTimerTask task = new RebalanceTimerTask();
		try {
			m_timer = new Timer();
			m_timer.scheduleAtFixedRate(task,
					m_config.getRebalanceableWaitInMs() + 2000,
					m_config.getRebalanceInterval());
		} catch (IllegalStateException ie) {
			LOGGER.error(
					"Errors when schedule timer for KafkaController.");
		}
		if (LOGGER.isInfoEnabled())
			LOGGER.info(
					"RebalanceController is established, timer is scheduled.");
	}

	private void cancelTimer() {
		if (m_timer != null) {
			m_timer.cancel();
			m_timer = null;
			if (LOGGER.isInfoEnabled())
				LOGGER.info(
						"Rebalancer is shutdown, timer is canceled.");
		}
	}

	private void noticeZkReconnected() {
		try {
			for (String name : m_kafkaConsumers.keySet()) {
				KafkaConsumer task = m_kafkaConsumers.get(name);
				task.zkReconnected();
			}
		} catch (Throwable th) {
			LOGGER.error(
					"Error occurs when noticing kafka consumers about zk reconnected.");
		}
	}

	private void startAllConsumers() {
		if (m_kafkaConsumers == null || m_kafkaConsumers.isEmpty()) {
			if (LOGGER.isInfoEnabled())
				LOGGER.info( "No kafkaConsumer needs to be started.");
			return;
		}
		for (String name : m_kafkaConsumers.keySet()) {
			KafkaConsumer consumer = m_kafkaConsumers.get(name);
			try {
				consumer.start();
			} catch (Exception e) {
				LOGGER.error( "Error occurs when starting consumer "
						+ name, e);
			}
		}
	}

	private void stopAllConsumers() {
		if (m_kafkaConsumers == null || m_kafkaConsumers.isEmpty()) {
			if (LOGGER.isInfoEnabled())
				LOGGER.info( "No kafkaConsumer needs to be stopped.");
			return;
		}
		for (String name : m_kafkaConsumers.keySet()) {
			KafkaConsumer consumer = m_kafkaConsumers.get(name);
			try {
				consumer.stop();
			} catch (Exception e) {
				LOGGER.error( "Error occurs when stopping consumer "
						+ name, e);
			}
		}
	}

	/** register & unregister **/
	public void register(String name, KafkaConsumer kafkaConsumer) {
		if (this.m_kafkaConsumers == null) {
			this.m_kafkaConsumers = new ConcurrentHashMap<String, KafkaConsumer>();
		}
		this.m_kafkaConsumers.put(name, kafkaConsumer);

		if (LOGGER.isInfoEnabled())
			LOGGER.info( "Register " + name + " to KafkaController. ");
	}

	public void unregister(String name) {
		if (this.m_kafkaConsumers == null) {
			return;
		}
		this.m_kafkaConsumers.remove(name);

		if (LOGGER.isInfoEnabled())
			LOGGER.info( "Unregister " + name
					+ " from KafkaController. ");
	}

	public boolean isRebalanceable() {
		return m_rebalanceable.get();
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}

	class OnConnectedTask implements Runnable {

		@Override
		public void run() {
			try {
				if (LOGGER.isInfoEnabled())
					LOGGER.info(
							"It will be rebalanceable only after waiting for all other nodes reconnected.");
				Thread.sleep(m_config.getRebalanceableWaitInMs());
			} catch (InterruptedException e) {
			}
			m_rebalanceable.set(true);
			if (LOGGER.isInfoEnabled())
				LOGGER.info( "Rebalanceable is set to true.");
		}

	}

	class RebalanceTimerTask extends TimerTask {

		@Override
		public void run() {
			if (!zkConnector.isZkConnected() || !isRebalanceable()) {
				if (LOGGER.isInfoEnabled())
					LOGGER.info( "Not rebalanceable this round.");
				return;
			}

			if (m_kafkaConsumers == null || m_kafkaConsumers.isEmpty()) {
				if (LOGGER.isInfoEnabled())
					LOGGER.info(
							"No kafkaConsumer needs to be rebalanced this round.");
				return;
			}
			if (LOGGER.isInfoEnabled())
				LOGGER.info( "Start to do rebalance.");
			for (String name : m_kafkaConsumers.keySet()) {
				KafkaConsumer task = m_kafkaConsumers.get(name);
				try {

					task.coordinate();

					Map<String, Integer> countMap = task.calcRebalance();
					if (countMap == null) {
						if (LOGGER.isInfoEnabled())
							LOGGER.info(
									"No need to do rebalance for " + name
											+ " in this round.");
						continue;
					}

					for (String topic : countMap.keySet()) {
						int count = countMap.get(topic);
						if (count > 0) {
							if (LOGGER.isInfoEnabled())
								LOGGER.info( "Try to take " + count
										+ " partitions for " + name
										+ ", topic " + topic);
							task.takePartitions(topic, count);

						} else if (count < 0) {
							count = Math.abs(count);
							if (LOGGER.isInfoEnabled())
								LOGGER.info( "Try to release "
										+ count + " partitions for " + name
										+ ", topic " + topic);
							task.releasePartitions(topic, count);
						}
					}
				} catch (Exception e) {
					LOGGER.error(
							"Error occurs during rebalance of " + name, e);
				}
			}
		}

	}

}
