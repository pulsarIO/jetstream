/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.progress;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.processor.hdfs.ProgressStore;
import com.ebay.jetstream.event.processor.hdfs.util.ZkConnector;

/**
 * @author weifang
 * 
 */
public class ZkProgressStore extends AbstractNamedBean implements
		InitializingBean, ShutDownable, ProgressStore {

	// injected
	private String zkHosts;
	private int connectionTimeoutMs;
	private int sessionTimeoutMs;
	private int retryTimes;
	private int sleepMsBetweenRetries;

	// internal
	private ZkConnector connector;

	public void setZkHosts(String zkHosts) {
		this.zkHosts = zkHosts;
	}

	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}

	public void setSessionTimeoutMs(int sessionTimeoutMs) {
		this.sessionTimeoutMs = sessionTimeoutMs;
	}

	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}

	public void setSleepMsBetweenRetries(int sleepMsBetweenRetries) {
		this.sleepMsBetweenRetries = sleepMsBetweenRetries;
	}

	@Override
	public void shutDown() {
		if (connector != null) {
			connector.close();
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		connector = new ZkConnector(zkHosts, connectionTimeoutMs,
				sessionTimeoutMs, retryTimes, sleepMsBetweenRetries);
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressStore#exists(java.lang
	 * .String)
	 */
	@Override
	public boolean exists(String path) {
		return connector.exists(path);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressStore#delete(java.lang
	 * .String)
	 */
	@Override
	public void delete(String path) {
		connector.delete(path);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressStore#getChildren(java
	 * .lang.String)
	 */
	@Override
	public List<String> getChildren(String path) {
		return connector.getChildren(path);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressStore#readBytes(java.
	 * lang.String)
	 */
	@Override
	public byte[] readBytes(String path) {
		return connector.readBytes(path);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressStore#readMap(java.lang
	 * .String)
	 */
	@Override
	public Map<String, Object> readMap(String path) {
		return connector.readJSON(path);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressStore#writeBytes(java
	 * .lang.String, byte[])
	 */
	@Override
	public void writeBytes(String path, byte[] bytes) {
		connector.writeBytes(path, bytes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressStore#writeMap(java.lang
	 * .String, java.util.Map)
	 */
	@Override
	public void writeMap(String path, Map<String, Object> map) {
		connector.writeJSON(path, map);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.processor.hdfs.ProgressStore#available()
	 */
	@Override
	public boolean available() {
		return connector.isConnected();
	}

}
