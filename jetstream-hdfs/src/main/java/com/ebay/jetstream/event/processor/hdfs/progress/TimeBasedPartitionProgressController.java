/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.progress;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.PartitionKey;
import com.ebay.jetstream.event.processor.hdfs.PartitionProgressController;
import com.ebay.jetstream.event.processor.hdfs.util.DateUtil;

/**
 * @author weifang
 * 
 */
public class TimeBasedPartitionProgressController implements
		PartitionProgressController {
	private static final Logger LOGGER = Logger
			.getLogger(TimeBasedPartitionProgressController.class.getName());

	// construct
	protected TimeBasedProgressControllerConfig config;
	protected TimeBasedProgressHelper helper;
	protected PartitionKey partitionKey;

	// metrics
	protected long curTimeSlot;
	protected int countInCurTimeSlot = 0;
	protected int countValid = 0;

	// stats
	protected long minTimestamp;
	protected long maxTimestamp;
	protected long eventCount;
	protected long totalLatency;

	public TimeBasedPartitionProgressController(
			TimeBasedProgressControllerConfig config, //
			TimeBasedProgressHelper helper, //
			PartitionKey partitionKey) {
		this.config = config;
		this.helper = helper;
		this.partitionKey = partitionKey;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressController#onNewFile(
	 * com.ebay.jetstream.event.processor.hdfs.PartitionKey,
	 * java.util.Collection)
	 */
	@Override
	public String onNewFile(Collection<JetstreamEvent> firstBatch) {
		curTimeSlot = genTimeSlot(firstBatch);
		String folderPath = helper.getFolder(curTimeSlot);
		updateStoredTimeSlot(partitionKey);
		return folderPath;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.ProgressController#onDropFile()
	 */
	@Override
	public void onDropFile() {
		clearStats();
	}

	@Override
	public void onStartBatch() {
		countInCurTimeSlot = 0;
		countValid = 0;
	}

	/**
	 * sampling the events and check the timestamp, to determine which timeslot
	 * those events should be in.
	 * 
	 * @param events
	 * @return
	 */
	protected long genTimeSlot(Collection<JetstreamEvent> events) {
		Map<Long, Integer> timeSlotMap = new HashMap<Long, Integer>();
		Iterator<JetstreamEvent> it = events.iterator();
		int index = 0;
		while (it.hasNext()) {
			JetstreamEvent event = it.next();
			if (index % config.getEventSampleFactor() == 0) {
				long timeSlot = getEventTimeSlot(event);
				if (timeSlotMap.containsKey(timeSlot)) {
					int count = timeSlotMap.get(timeSlot);
					timeSlotMap.put(timeSlot, count + 1);
				} else {
					timeSlotMap.put(timeSlot, 1);
				}
			}
			index++;
		}
		Entry<Long, Integer> max = null;
		for (Entry<Long, Integer> entry : timeSlotMap.entrySet()) {
			if (max == null)
				max = entry;
			else if (max.getValue() < entry.getValue()) {
				max = entry;
			}
		}
		return max.getKey();
	}

	/**
	 * if event has timestamp key, get timestamp and calc time slot; if not,
	 * clac time slot with current time in millis
	 */
	protected long getEventTimeSlot(JetstreamEvent event) {
		String tsKey = config.getTimestampKey();
		long interval = config.getFolderIntervalInMs();
		if (tsKey != null) {
			Long ts = (Long) event.get(tsKey);
			if (ts != null) {
				// found timestamp property in the event
				return ts / interval * interval;
			}
		}
		// not found timestamp property, use the current system time
		return System.currentTimeMillis() / interval * interval;
	}

	@Override
	public void onNextEvent(JetstreamEvent event) {
		countValid++;
		if (getEventTimeSlot(event) == curTimeSlot)
			countInCurTimeSlot++;

		String tsKey = config.getTimestampKey();
		if (tsKey != null) {
			Long ts = (Long) event.get(tsKey);
			if (ts != null) {
				if (minTimestamp > ts)
					minTimestamp = ts;
				if (maxTimestamp < ts)
					maxTimestamp = ts;
				eventCount++;
				long v = System.currentTimeMillis() - ts;
				totalLatency += v;
			}
		}
	}

	@Override
	public boolean onEndBatch() {
		// if more than half events not in current time slot, tell the src to
		// commit
		float ratio = config.getMoveToNextRatio();
		if (countInCurTimeSlot < countValid * ratio) {
			LOGGER.info("More than "
					+ ratio
					+ " events are not in current time slot. Should move to the next folder.");
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean onCommitFile(String dstFilePath, Map<String, Object> stats) {
		try {
			// log fileStats
			helper.writeStats(curTimeSlot, dstFilePath, stats);
			return true;
		} finally {
			clearStats();
		}
	}

	protected void clearStats() {
		minTimestamp = Long.MAX_VALUE;
		maxTimestamp = 0L;
		eventCount = 0L;
		totalLatency = 0L;
	}

	protected void updateStoredTimeSlot(PartitionKey key) {
		try {
			long storedTs = helper.readWorkingTimeSlot(key.getTopic(),
					key.getPartition());
			if (storedTs == 0 || curTimeSlot != storedTs) {
				helper.writeWorkingTimeSlot(curTimeSlot, key.getTopic(),
						key.getPartition());
			}
		} catch (Exception e) {
			LOGGER.log(
					Level.SEVERE,
					"Fail to update current timeSlot to store for "
							+ key.toString() + " "
							+ DateUtil.formatDateForLog(curTimeSlot)
							+ ", will try it next time.");
		}
	}
}
