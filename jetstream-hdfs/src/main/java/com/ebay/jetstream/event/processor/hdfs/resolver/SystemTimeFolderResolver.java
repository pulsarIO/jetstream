/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.resolver;

import java.text.ParseException;
import java.util.Collection;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.FolderResolver;
import com.ebay.jetstream.event.processor.hdfs.util.DateUtil;

/**
 * @author weifang
 * 
 */
public class SystemTimeFolderResolver implements FolderResolver {
	// injected
	private long folderIntervalInMs = 3600000;
	private String folderPathFormat = "yyyyMMdd/HH_mm";

	public void setFolderIntervalInMs(long folderIntervalInMs) {
		this.folderIntervalInMs = folderIntervalInMs;
	}

	public void setFolderPathFormat(String folderPathFormat) {
		this.folderPathFormat = folderPathFormat;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.FolderResolver#resolveFolder(
	 * java.util.Collection, java.lang.String)
	 */
	@Override
	public String getCurrentFolder(Collection<JetstreamEvent> events) {
		long timeSlot = System.currentTimeMillis() / folderIntervalInMs
				* folderIntervalInMs;
		return DateUtil.formatDate(timeSlot, folderPathFormat);
	}

	@Override
	public boolean shouldMoveToNext(Collection<JetstreamEvent> lastBatch,
			String currentFolder) {
		try {
			long maxTs = DateUtil.parseDate(currentFolder, folderPathFormat)
					.getTime() + folderIntervalInMs;
			return System.currentTimeMillis() > maxTs;
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
}
