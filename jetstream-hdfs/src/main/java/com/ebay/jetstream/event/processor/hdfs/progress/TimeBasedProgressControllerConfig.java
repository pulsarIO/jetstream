/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.progress;

import java.util.List;

/**
 * @author weifang
 * 
 */
public class TimeBasedProgressControllerConfig {
	private String timestampKey;
	private long folderIntervalInMs = 3600000;
	private float moveToNextRatio = 0.3f;
	private String folderPathFormat = "yyyyMMdd/HH_mm";
	private int eventSampleFactor = 50;
	private String identifier;
	private String dataCenter;
	private List<String> totalTopics;
	private List<String> totalDataCenters;
	private int successCheckCount = 6;
	private String successFileName = "_SUCCESS";

	public String getTimestampKey() {
		return timestampKey;
	}

	public void setTimestampKey(String timestampKey) {
		this.timestampKey = timestampKey;
	}

	public long getFolderIntervalInMs() {
		return folderIntervalInMs;
	}

	public void setFolderIntervalInMs(long folderIntervalInMs) {
		this.folderIntervalInMs = folderIntervalInMs;
	}

	public String getFolderPathFormat() {
		return folderPathFormat;
	}

	public void setFolderPathFormat(String folderPathFormat) {
		this.folderPathFormat = folderPathFormat;
	}

	public int getEventSampleFactor() {
		return eventSampleFactor;
	}

	public void setEventSampleFactor(int eventSampleFactor) {
		this.eventSampleFactor = eventSampleFactor;
	}

	public float getMoveToNextRatio() {
		return moveToNextRatio;
	}

	public void setMoveToNextRatio(float moveToNextRatio) {
		this.moveToNextRatio = moveToNextRatio;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public String getDataCenter() {
		return dataCenter;
	}

	public void setDataCenter(String dataCenter) {
		this.dataCenter = dataCenter;
	}

	public List<String> getTotalTopics() {
		return totalTopics;
	}

	public void setTotalTopics(List<String> totalTopics) {
		this.totalTopics = totalTopics;
	}

	public List<String> getTotalDataCenters() {
		return totalDataCenters;
	}

	public void setTotalDataCenters(List<String> totalDataCenters) {
		this.totalDataCenters = totalDataCenters;
	}

	public int getSuccessCheckCount() {
		return successCheckCount;
	}

	public void setSuccessCheckCount(int successCheckCount) {
		this.successCheckCount = successCheckCount;
	}

	public String getSuccessFileName() {
		return successFileName;
	}

	public void setSuccessFileName(String successFileName) {
		this.successFileName = successFileName;
	}

}
