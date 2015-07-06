/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs.resolver;

import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.FolderResolver;
import com.ebay.jetstream.event.processor.hdfs.util.DateUtil;

/**
 * @author weifang
 * 
 */
public class EventTimestampFolderResolver implements FolderResolver {
	// injected
	private String timestampKey;
	private long folderIntervalInMs = 3600000;
	private float moveToNextRatio = 0.2f;
	private String folderPathFormat = "yyyyMMdd/HH_mm";
	private int eventSampleFactor = 1;

	public String getTimestampKey() {
		return timestampKey;
	}

	public long getFolderIntervalInMs() {
		return folderIntervalInMs;
	}

	public float getMoveToNextRatio() {
		return moveToNextRatio;
	}

	public String getFolderPathFormat() {
		return folderPathFormat;
	}

	public int getEventSampleFactor() {
		return eventSampleFactor;
	}

	public void setTimestampKey(String timestampKey) {
		this.timestampKey = timestampKey;
	}

	public void setFolderIntervalInMs(long folderIntervalInMs) {
		this.folderIntervalInMs = folderIntervalInMs;
	}

	public void setMoveToNextRatio(float moveToNextRatio) {
		this.moveToNextRatio = moveToNextRatio;
	}

	public void setFolderPathFormat(String folderPathFormat) {
		this.folderPathFormat = folderPathFormat;
	}

	public void setEventSampleFactor(int eventSampleFactor) {
		this.eventSampleFactor = eventSampleFactor;
	}

	@Override
	public String getCurrentFolder(Collection<JetstreamEvent> thisBatch) {
		Map<Long, Integer> timeSlotMap = new HashMap<Long, Integer>();
		Iterator<JetstreamEvent> it = thisBatch.iterator();
		int index = 0;
		while (it.hasNext()) {
			JetstreamEvent event = it.next();
			if (index % eventSampleFactor == 0) {
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
		long timeSlot = max.getKey();
		return DateUtil.formatDate(timeSlot, folderPathFormat);
	}

	protected long getEventTimeSlot(JetstreamEvent event) {
		Long ts = null;
		if (timestampKey != null) {
			ts = (Long) event.get(timestampKey);
		}

		// not found timestamp property, use the current system time
		if (ts == null) {
			ts = System.currentTimeMillis();
		}
		return ts / folderIntervalInMs * folderIntervalInMs;
	}

	@Override
	public boolean shouldMoveToNext(Collection<JetstreamEvent> lastBatch,
			String currentFolder) {
		try {
			Iterator<JetstreamEvent> it = lastBatch.iterator();
			long timeslot = DateUtil.parseDate(currentFolder, folderPathFormat)
					.getTime();
			int index = 0;
			int total = 0;
			int next = 0;
			while (it.hasNext()) {
				JetstreamEvent event = it.next();
				if (index % eventSampleFactor == 0) {
					total++;
					if (getEventTimeSlot(event) != timeslot) {
						next++;
					}
				}
				index++;
			}
			return (float) next / total > moveToNextRatio;
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
}
