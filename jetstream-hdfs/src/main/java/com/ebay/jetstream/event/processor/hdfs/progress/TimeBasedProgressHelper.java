/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.progress;

import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ebay.jetstream.event.processor.hdfs.ProgressStore;
import com.ebay.jetstream.event.processor.hdfs.util.DateUtil;
import com.ebay.jetstream.event.processor.hdfs.util.MiscUtil;

/**
 * @author weifang
 * 
 */
public class TimeBasedProgressHelper {
	private static final Logger LOGGER = Logger
			.getLogger(TimeBasedProgressHelper.class.getName());

	public static final String PATH_ROOT = "/js_hdfs";
	public static final String PATH_TIMESLOTS = "timeslots";
	public static final String PATH_STATS = "stats";
	public static final String TS_PATH_FORMAT = "yyyyMMdd_HHmmss";
	public static final String KEY_WORKING_TIMESLOT = "workingTimeSlot";
	public static final String KEY_HOST_NAME = "hostName";
	public static final String STATS_SUFFIX = ".stats";

	private TimeBasedProgressControllerConfig config;
	private ProgressStore store;

	public TimeBasedProgressHelper(TimeBasedProgressControllerConfig config,
			ProgressStore store) {
		this.config = config;
		this.store = store;
	}

	public long readWorkingTimeSlot(String topic, int partition) {
		return readWorkingTimeSlot(topic, config.getDataCenter(), partition);
	}

	public long readWorkingTimeSlot(String topic, String dataCenter,
			int partition) {
		String path = getTimeSlotPath(topic, dataCenter, partition);
		Map<String, Object> map = store.readMap(path);
		if (map == null || !map.containsKey(KEY_WORKING_TIMESLOT))
			return 0;
		String curTsStr = (String) map.get(KEY_WORKING_TIMESLOT);
		if (curTsStr != null) {
			return Long.valueOf(curTsStr);
		}
		return 0;
	}

	public void writeWorkingTimeSlot(long curTimeSlot, String topic,
			int partition) {
		String path = getTimeSlotPath(topic, config.getDataCenter(), partition);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(KEY_HOST_NAME, MiscUtil.getLocalHostName());
		map.put(KEY_WORKING_TIMESLOT, String.valueOf(curTimeSlot));
		store.writeMap(path, map);
	}

	public String getTimeSlotPath(String topic) {
		StringBuffer sb = new StringBuffer(PATH_ROOT);
		sb.append("/").append(PATH_TIMESLOTS).append("/")
				.append(config.getIdentifier()).append("/").append(topic);
		return sb.toString();
	}

	public String getTimeSlotPath(String topic, String dataCenter, int partition) {
		StringBuffer sb = new StringBuffer();
		sb.append(getTimeSlotPath(topic)).append("/").append(dataCenter)
				.append("/").append(partition);
		return sb.toString();
	}

	public String getStatsPath(long timeSlot) {
		String tsPart = DateUtil.formatDate(timeSlot, TS_PATH_FORMAT);
		StringBuffer sb = new StringBuffer(PATH_ROOT);
		sb.append("/").append(PATH_STATS).append("/")
				.append(config.getIdentifier()).append("/").append(tsPart);
		return sb.toString();
	}

	public Map<String, Map<String, Object>> readFileStats(long timeSlot) {
		String tsPath = getStatsPath(timeSlot);
		List<String> children = store.getChildren(tsPath);
		Map<String, Map<String, Object>> ret = new LinkedHashMap<String, Map<String, Object>>();
		if (children != null) {
			for (String child : children) {
				String fullPath = tsPath + "/" + child;
				Map<String, Object> chMap = store.readMap(fullPath);
				ret.put(child, chMap);
			}
		}
		return ret;
	}

	public void deleteFileStats(long timeSlot) {
		String path = getStatsPath(timeSlot);
		try {
			List<String> children = store.getChildren(path);
			if (children != null) {
				for (String child : children) {
					store.delete(path + "/" + child);
				}
			}
			store.delete(path);
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Fail to delete stats for " + path, e);
		}
	}

	public void writeStats(long timeSlot, String fileName,
			Map<String, Object> stats) {
		String path = getStatsPath(timeSlot);
		path += "/" + fileName + STATS_SUFFIX;
		store.writeMap(path, stats);
	}

	/**
	 * get min working folder date (check success until min working) relate to
	 * topics
	 */
	public long getMinWorking() {
		long minFolderTs = 0;
		for (String topic : config.getTotalTopics()) {
			String tpath = getTimeSlotPath(topic);
			if (!store.exists(tpath))
				continue;
			for (String dc : config.getTotalDataCenters()) {
				String dcPath = tpath + "/" + dc;
				if (!store.exists(dcPath))
					continue;
				List<String> pstrs = store.getChildren(dcPath);
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

	public String getFolder(long timeSlot) {
		String folderPath = DateUtil.formatDate(timeSlot,
				config.getFolderPathFormat());
		return folderPath;
	}

	public long getTimeSlot(String folder) throws ParseException {
		long ts = DateUtil.parseDate(folder, config.getFolderPathFormat())
				.getTime();
		return ts;
	}

	public static void main(String[] args) {
		// Map<String, Map<String, Object>> map = new LinkedHashMap<String,
		// Map<String, Object>>();
		// Map<String, Object> map2 = new LinkedHashMap<String, Object>();
		// map2.put("a1", "bbb");
		// map2.put("a2", 12354L);
		// map.put("m1", map2);
		// String str = new String(JsonUtil.mapToJsonBytes((Map) map));
		// Map<String, Object> map3 = JsonUtil
		// .jsonStringToMap("{\"m1\":{\"a1\":\"bbb\",\"a2\":12355}}");
		// System.out.println(map3);
	}
}
