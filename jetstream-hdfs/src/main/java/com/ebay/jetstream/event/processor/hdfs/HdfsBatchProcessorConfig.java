/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author weifang
 * 
 */
public class HdfsBatchProcessorConfig extends AbstractNamedBean implements
		XSerializable {
	private String outputFolder;
	private String workingFolder;
	private String errorFolder;
	private String fileNamePrefix = "";
	private String fileNameSuffix = "";
	private long waitForFsAvaliableInMs = 60000;
	private boolean logErrorEvents = true;
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

	public String getFileNamePrefix() {
		return fileNamePrefix;
	}

	public void setFileNamePrefix(String fileNamePrefix) {
		this.fileNamePrefix = fileNamePrefix;
	}

	public String getFileNameSuffix() {
		return fileNameSuffix;
	}

	public void setFileNameSuffix(String fileNameSuffix) {
		this.fileNameSuffix = fileNameSuffix;
	}

}
