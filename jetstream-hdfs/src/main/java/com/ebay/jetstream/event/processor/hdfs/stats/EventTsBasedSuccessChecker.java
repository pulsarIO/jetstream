/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.stats;

import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.HdfsClient;
import com.ebay.jetstream.event.processor.hdfs.PartitionKey;
import com.ebay.jetstream.event.processor.hdfs.resolver.EventTimestampFolderResolver;
import com.ebay.jetstream.event.processor.hdfs.util.DateUtil;
import com.ebay.jetstream.event.processor.hdfs.util.JsonUtil;
import com.ebay.jetstream.event.processor.hdfs.util.MiscUtil;
import com.ebay.jetstream.event.processor.hdfs.util.ZkConnector;
import com.ebay.jetstream.messaging.MessageServiceTimer;

/**
 * @author weifang
 * 
 */
public class EventTsBasedSuccessChecker extends EventTsBasedStatsRecorder
		implements InitializingBean, ShutDownable {
	public static final String PATH_ROOT = "/js_hdfs";
	public static final String PATH_TIMESLOTS = "timeslots";
	public static final String PATH_STATS = "stats";
	public static final String TS_PATH_FORMAT = "yyyyMMdd_HHmmss";
	public static final String KEY_WORKING_TIMESLOT = "workingTimeSlot";
	public static final String KEY_HOST_NAME = "hostName";
	public static final String STATS_SUFFIX = ".stats";

	private static final Logger LOGGER = Logger
			.getLogger(EventTsBasedSuccessChecker.class.getName());

	// injected
	private EventTsBasedSuccessCheckerConfig config;
	private EventTimestampFolderResolver folderResolver;
	private HdfsClient hdfs;

	// internal
	protected ZkConnector zkConnector;
	protected SuccessTask successTask;

	public void setConfig(EventTsBasedSuccessCheckerConfig config) {
		this.config = config;
	}

	public void setFolderResolver(EventTimestampFolderResolver folderResolver) {
		this.folderResolver = folderResolver;
	}

	public void setHdfs(HdfsClient hdfs) {
		this.hdfs = hdfs;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		zkConnector = new ZkConnector(config.getZkHosts(), //
				config.getZkConnectionTimeoutMs(), //
				config.getZkSessionTimeoutMs(), //
				config.getZkRetryTimes(), //
				config.getZkSleepMsBetweenRetries());

		successTask = new SuccessTask();
		long interval = config.getSuccessCheckInterval();
		MessageServiceTimer.sInstance().schedulePeriodicTask(successTask,
				interval, interval);
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}

	@Override
	public void shutDown() {
		if (successTask != null) {
			successTask.cancel();
		}
		if (zkConnector != null) {
			zkConnector.close();
		}
	}

	@Override
	protected Long getTimestamp(JetstreamEvent event) {
		if (folderResolver.getTimestampKey() == null) {
			return null;
		} else {
			return (Long) event.get(folderResolver.getTimestampKey());
		}
	}

	@Override
	protected boolean commitStats(PartitionKey key, long startOffset,
			long endOffset, String folder, String destFileName, BaseStats stats) {
		try {
			long timeSlot = DateUtil.parseDate(folder,
					folderResolver.getFolderPathFormat()).getTime();
			String path = getStatsPath(timeSlot);
			path += "/" + destFileName + STATS_SUFFIX;
			zkConnector.writeJSON(
					path,
					genFileStats(key, startOffset, endOffset,
							(EventTsBasedStats) stats));
			return true;
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			return false;
		}
	}

	@Override
	public synchronized void onFileCreated(PartitionKey key, long startOffset,
			String folder, String tmpFileName) {
		super.onFileCreated(key, startOffset, folder, tmpFileName);
		try {
			writeWorkingTimeSlot(
					key,
					DateUtil.parseDate(folder,
							folderResolver.getFolderPathFormat()).getTime());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	protected List<String> getSuccessCheckFolders() {
		List<String> ret = new LinkedList<String>();
		long minFolderTs = getMinWorking();
		if (minFolderTs == 0) {
			LOGGER.log(Level.INFO,
					"No working log util now. Wait to check success in next round.");
			return ret;
		}
		long folderTs = minFolderTs;
		int checkRange = config.getSuccessCheckCount();
		for (int i = 0; i < checkRange; i++) {
			folderTs = folderTs - folderResolver.getFolderIntervalInMs();
			ret.add(DateUtil.formatDate(folderTs,
					folderResolver.getFolderPathFormat()));
		}

		return ret;
	}

	protected long getMinWorking() {
		long minFolderTs = 0;
		for (String topic : config.getTotalTopics()) {
			String tpath = getTimeSlotPath(topic);
			if (!zkConnector.exists(tpath))
				continue;
			for (String dc : config.getTotalDataCenters()) {
				String dcPath = tpath + "/" + dc;
				if (!zkConnector.exists(dcPath))
					continue;
				List<String> pstrs = zkConnector.getChildren(dcPath);
				if (pstrs == null)
					continue;
				for (String pstr : pstrs) {
					int partition = Integer.parseInt(pstr);
					try {
						long workTs = readWorkingTimeSlot(topic, dc, partition);
						if (workTs != 0) {
							if (minFolderTs == 0)
								minFolderTs = workTs;
							else if (minFolderTs > workTs)
								minFolderTs = workTs;
						}
					} catch (Exception e) {
						continue;
					}
				}
			}
		}
		return minFolderTs;
	}

	protected String getTimeSlotPath(String topic) {
		StringBuffer sb = new StringBuffer(PATH_ROOT);
		sb.append("/").append(PATH_TIMESLOTS).append("/")
				.append(config.getIdentifier()).append("/").append(topic);
		return sb.toString();
	}

	protected String getTimeSlotPath(String topic, String dataCenter,
			int partition) {
		StringBuffer sb = new StringBuffer();
		sb.append(getTimeSlotPath(topic)).append("/").append(dataCenter)
				.append("/").append(partition);
		return sb.toString();
	}

	protected String getStatsPath(long timeSlot) {
		String tsPart = DateUtil.formatDate(timeSlot, TS_PATH_FORMAT);
		StringBuffer sb = new StringBuffer(PATH_ROOT);
		sb.append("/").append(PATH_STATS).append("/")
				.append(config.getIdentifier()).append("/").append(tsPart);
		return sb.toString();
	}

	public long readWorkingTimeSlot(String topic, int partition) {
		return readWorkingTimeSlot(topic, config.getDataCenter(), partition);
	}

	public long readWorkingTimeSlot(String topic, String dataCenter,
			int partition) {
		String path = getTimeSlotPath(topic, dataCenter, partition);
		Map<String, Object> map = zkConnector.readJSON(path);
		if (map == null || !map.containsKey(KEY_WORKING_TIMESLOT))
			return 0;
		String curTsStr = (String) map.get(KEY_WORKING_TIMESLOT);
		if (curTsStr != null) {
			return Long.valueOf(curTsStr);
		}
		return 0;
	}

	public void writeWorkingTimeSlot(PartitionKey key, long curTimeSlot) {
		String path = getTimeSlotPath(key.getTopic(), config.getDataCenter(),
				key.getPartition());
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(KEY_HOST_NAME, MiscUtil.getLocalHostName());
		map.put(KEY_WORKING_TIMESLOT, String.valueOf(curTimeSlot));
		zkConnector.writeJSON(path, map);
	}

	public String getSuccessPath(String folder) {
		return config.getOutputFolder() + "/" + folder + "/"
				+ config.getSuccessFileName();
	}

	protected boolean isSuccess(String folder) {
		try {
			if (!hdfs.exist(config.getOutputFolder() + "/" + folder)) {
				return false;
			} else {
				return true;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected Map<String, Map<String, Object>> getFileStats(String folder) {
		try {
			if (hdfs.exist(getSuccessPath(folder))) {
				// already created success file, just return null to let the
				// caller skip this folder
				return null;
			}
			long ts = getTimeSlot(folder);
			return readFileStats(ts);
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, "Unrecognized folder " + folder, e);
			return null;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString(), e);
			return null;
		}
	}

	protected Map<String, Map<String, Object>> readFileStats(long timeSlot) {
		String tsPath = getStatsPath(timeSlot);
		List<String> children = zkConnector.getChildren(tsPath);
		Map<String, Map<String, Object>> ret = new LinkedHashMap<String, Map<String, Object>>();
		if (children != null) {
			for (String child : children) {
				String fullPath = tsPath + "/" + child;
				Map<String, Object> chMap = zkConnector.readJSON(fullPath);
				ret.put(child, chMap);
			}
		}
		return ret;
	}

	protected Map<String, Object> genFileStats(PartitionKey partitionKey,
			long startOffset, long endOffset, EventTsBasedStats stats) {
		Map<String, Object> statsMap = new LinkedHashMap<String, Object>();
		statsMap.put("hostName", MiscUtil.getLocalHostName());
		statsMap.put("topic", partitionKey.getTopic());
		statsMap.put("partition", partitionKey.getPartition());
		statsMap.put("startOffset", startOffset);
		statsMap.put("endOffset", endOffset);
		statsMap.put("eventCount", stats.getEventCount());
		statsMap.put("errorCount", stats.getErrorCount());

		if (stats.getLoadStartTime() != Long.MAX_VALUE)
			statsMap.put("loadStartTime", stats.getLoadStartTime());
		else
			statsMap.put("loadStartTime", 0L);
		statsMap.put("loadEndTime", stats.getLoadEndTime());
		if (stats.getMinTimestamp() != Long.MAX_VALUE)
			statsMap.put("minTimestamp", stats.getMinTimestamp());
		else
			statsMap.put("minTimestamp", 0L);
		statsMap.put("maxTimestamp", stats.getMaxTimestamp());
		statsMap.put("avgLatencyInMs",
				stats.getTotalLatency() / stats.getEventCount());
		return statsMap;
	}

	protected void deleteFileStats(long timeSlot) {
		String path = getStatsPath(timeSlot);
		try {
			List<String> children = zkConnector.getChildren(path);
			if (children != null) {
				for (String child : children) {
					zkConnector.delete(path + "/" + child);
				}
			}
			zkConnector.delete(path);
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Fail to delete stats for " + path, e);
		}
	}

	protected long getTimeSlot(String folder) throws ParseException {
		long ts = DateUtil.parseDate(folder,
				folderResolver.getFolderPathFormat()).getTime();
		return ts;
	}

	protected void aggregateStats(Map<String, Map<String, Object>> fileStats,
			Map<String, Object> aggregatedStats) {
		long eventCount = 0;
		long errorCount = 0;
		long firstLoadStartTime = Long.MAX_VALUE;
		long lastLoadEndTime = 0;
		long avgLatencyInMs = 0;
		long minTimestamp = Long.MAX_VALUE;
		long maxTimestamp = 0;
		Number n = null;
		int fileCount = 0;
		for (Entry<String, Map<String, Object>> entry : fileStats.entrySet()) {
			Map<String, Object> stats = entry.getValue();
			fileCount++;
			n = (Number) stats.get("eventCount");
			if (n != null) {
				eventCount += n.longValue();
			}
			n = (Number) stats.get("errorCount");
			if (n != null) {
				errorCount += n.longValue();
			}
			n = (Number) stats.get("loadStartTime");
			if (n != null && firstLoadStartTime > n.longValue()) {
				firstLoadStartTime = n.longValue();
			}
			n = (Number) stats.get("loadEndTime");
			if (n != null && lastLoadEndTime < n.longValue()) {
				lastLoadEndTime = n.longValue();
			}
			n = (Number) stats.get("minTimestamp");
			if (n != null && minTimestamp > n.longValue()) {
				minTimestamp = n.longValue();
			}
			n = (Number) stats.get("maxTimestamp");
			if (n != null && maxTimestamp < n.longValue()) {
				maxTimestamp = n.longValue();
			}
			n = (Number) stats.get("avgLatencyInMs");
			if (n != null) {
				avgLatencyInMs += n.longValue();
			}
		}

		aggregatedStats.put("fileCount", fileCount);
		aggregatedStats.put("totalEventCount", eventCount);
		aggregatedStats.put("totalErrorCount", errorCount);
		aggregatedStats.put("firstLoadStartTime", firstLoadStartTime);
		aggregatedStats.put("lastLoadEndTime", lastLoadEndTime);
		aggregatedStats.put("minTimestamp", minTimestamp);
		aggregatedStats.put("maxTimestamp", maxTimestamp);
		aggregatedStats.put("avgLatencyInMs", avgLatencyInMs / fileCount);
	}

	public void markSuccess(String folder,
			Map<String, Map<String, Object>> fileStats,
			Map<String, Object> aggStats) {
		OutputStream os = null;
		try {
			long ts = getTimeSlot(folder);
			Map<String, Object> ret = new LinkedHashMap<String, Object>();
			ret.put("overview", aggStats);
			ret.put("files", fileStats);
			os = hdfs.createFile(getSuccessPath(folder), true);
			JsonUtil.mapToJsonStream(ret, os);

			deleteFileStats(ts);
		} catch (ParseException e) {
			throw new RuntimeException("Unrecognized folder " + folder, e);
		} catch (IOException e) {
			throw new RuntimeException(
					"Failed to output success file for folder " + folder, e);
		} finally {
			if (os != null) {
				try {
					os.close();
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.toString(), e);
				}
			}
		}
	}

	class SuccessTask extends TimerTask {

		@Override
		public void run() {
			try {
				List<String> folders = getSuccessCheckFolders();
				for (String folder : folders) {
					try {
						if (isSuccess(folder)) {
							Map<String, Map<String, Object>> fileStats = getFileStats(folder);
							if (fileStats != null) {
								Map<String, Object> aggregated = new LinkedHashMap<String, Object>();
								aggregateStats(fileStats, aggregated);
								markSuccess(folder, fileStats, aggregated);
							}
						}
					} catch (Exception e) {
						LOGGER.log(Level.SEVERE,
								"Fail to check success for folder " + folder, e);
					}
				}
			} catch (Throwable th) {
				LOGGER.log(Level.SEVERE, th.toString(), th);
			}
		}
	}
}
