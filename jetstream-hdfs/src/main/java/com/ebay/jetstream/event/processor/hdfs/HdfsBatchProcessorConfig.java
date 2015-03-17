/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

/**
 * @author weifang
 * 
 */
public class HdfsBatchProcessorConfig {
	private String outputFolder;
	private String workingFolder;
	private String errorFolder;
	private long waitForFsAvaliableInMs = 60000;
	private long waitForFileCloseInMs = 2000;
	private boolean logErrorEvents = true;
	private long successCheckInterval = 300000;
	private String errorFileSuffix = ".error";

	public String getOutputFolder() {
		return outputFolder;
	}

	public void setOutputFolder(String outputFolder) {
		this.outputFolder = outputFolder;
	}

	public String getWorkingFolder() {
		return workingFolder;
	}

	public void setWorkingFolder(String workingFolder) {
		this.workingFolder = workingFolder;
	}

	public String getErrorFolder() {
		return errorFolder;
	}

	public void setErrorFolder(String statsFolder) {
		this.errorFolder = statsFolder;
	}

	public long getWaitForFsAvaliableInMs() {
		return waitForFsAvaliableInMs;
	}

	public void setWaitForFsAvaliableInMs(long waitForFsAvaliableInMs) {
		this.waitForFsAvaliableInMs = waitForFsAvaliableInMs;
	}

	public long getWaitForFileCloseInMs() {
		return waitForFileCloseInMs;
	}

	public void setWaitForFileCloseInMs(long waitForFileCloseInMs) {
		this.waitForFileCloseInMs = waitForFileCloseInMs;
	}

	public boolean isLogErrorEvents() {
		return logErrorEvents;
	}

	public void setLogErrorEvents(boolean logErrorEvents) {
		this.logErrorEvents = logErrorEvents;
	}

	public String getErrorFileSuffix() {
		return errorFileSuffix;
	}

	public void setErrorFileSuffix(String errorFileSuffix) {
		this.errorFileSuffix = errorFileSuffix;
	}

	public long getSuccessCheckInterval() {
		return successCheckInterval;
	}

	public void setSuccessCheckInterval(long successCheckInterval) {
		this.successCheckInterval = successCheckInterval;
	}

}
