/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.common.OffsetOutOfRangeException;

import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.util.MiscUtil;

/**
 * @author weifang
 * 
 */
public class PartitionWriter {
	private static final Logger LOGGER = Logger.getLogger(PartitionWriter.class
			.getName());

	// construct
	protected HdfsBatchProcessorConfig config;
	protected PartitionKey partitionKey;
	protected HdfsClient hdfs;
	protected PartitionProgressController progress;
	protected EventWriterFactory writerFactory;
	protected EventWriterFactory errorWriterFactory;
	protected FileNameResolver fileNameResolver;

	// internal
	protected EventWriter currentWriter;
	protected EventWriter currentErrorWriter;
	protected long startOffset = -1L;
	protected long lastOffset = -1L;
	protected String currentFolder;
	protected String currentTmpFile;
	protected String dstFilePath;
	protected String lastDstFilePath;

	// stats
	protected long eventCount = 0L;
	protected long errorCount = 0L;
	protected long loadStartTime = Long.MAX_VALUE;
	protected long loadEndTime = 0L;

	public PartitionWriter(HdfsBatchProcessorConfig config,//
			PartitionKey partitionKey, //
			HdfsClient hdfs, //
			PartitionProgressController progress, //
			EventWriterFactory writerFactory, //
			EventWriterFactory errorWriterFactory, //
			FileNameResolver fileNameResolver) {
		this.config = config;
		this.partitionKey = partitionKey;
		this.hdfs = hdfs;
		this.progress = progress;
		this.writerFactory = writerFactory;
		this.errorWriterFactory = errorWriterFactory;
		this.fileNameResolver = fileNameResolver;
	}

	public synchronized BatchResponse doBatch(long headOffset,
			Collection<JetstreamEvent> events, AtomicLong droppedCounter) {
		if (events.size() <= 0) {
			LOGGER.log(Level.INFO, partitionKey.toString()
					+ ": cannot do batch with empty event list.");
			return BatchResponse.getNextBatch();
		}
		long tailOffset = headOffset + events.size() - 1;
		if (lastOffset < 0) {
			lastOffset = headOffset - 1;
		}
		// startOffset - lastOffset > 1 means events between them are lost,
		// throw exception to roll back
		if (headOffset - lastOffset > 1) {
			return offsetError(partitionKey.toString() + ": events of offset "
					+ (lastOffset + 1) + " to offset " + (headOffset - 1)
					+ " is lost.");
		}
		if (currentWriter == null) {
			try {
				openFile(events);
			} catch (Exception e) {
				return dstError("Fail to open file. " + e.toString(), e);
			}
		}

		progress.onStartBatch();
		headOffset--;
		try {
			for (JetstreamEvent event : events) {
				headOffset++;
				if (headOffset - lastOffset < 1) {
					droppedCounter.incrementAndGet();
					continue;
				}

				boolean rs = currentWriter.write(event);
				if (!rs) {
					logErrorEvent(event);
					droppedCounter.incrementAndGet();
					errorCount++;
					continue;
				}

				progress.onNextEvent(event);
				eventCount++;
			}

		} catch (Exception e) {
			return dstError(
					"Error occurs during writing events. " + e.toString(), e);
		}

		lastOffset = tailOffset;

		if (progress.onEndBatch()) {
			try {
				commitFile();
				return BatchResponse.advanceAndGetNextBatch();
			} catch (Exception e) {
				return dstError("Fail to commit file. " + e.toString(), e);
			}
		} else {
			return BatchResponse.getNextBatch();
		}
	}

	public synchronized BatchResponse doException(Exception ex) {
		if (ex instanceof OffsetOutOfRangeException) {
			try {
				LOGGER.log(Level.INFO, "OffsetOutOfRangeException for "
						+ partitionKey.toString() + ", Try to commit file. ");
				// TODO
				// offset out of range after init could be a fatal error of
				// performance
				commitFile();
				return BatchResponse.advanceAndGetNextBatch();
			} catch (Exception e) {
				return dstError("Fail to commit file. " + e.toString(), e);
			}
		}

		LOGGER.log(Level.INFO,
				"Unknown exception happended on " + partitionKey.toString()
						+ ". " + ex.toString(), ex);

		// unknow exception, anyway drop
		if (currentWriter != null) {
			dropTmpFile();
			revertOffsets();
		} else {
			try {
				if (lastDstFilePath != null) {
					hdfs.delete(lastDstFilePath, false);
					LOGGER.log(Level.INFO, "Delete dstFile " + lastDstFilePath);
				}
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, "Fail to delete the dstFile "
						+ lastDstFilePath
						+ " which cannot be committed by upstream.", e);
			}
		}
		return BatchResponse.revertAndGetNextBatch();
	}

	public synchronized BatchResponse doIdle() {
		try {
			LOGGER.log(Level.INFO,
					"onIdle called for " + partitionKey.toString()
							+ ", commit file. ");
			commitFile();
			return BatchResponse.advanceAndGetNextBatch();
		} catch (Exception e) {
			return dstError("Fail to commit file. " + e.toString(), e);
		}
	}

	public synchronized BatchResponse doStreamTermination() {
		try {
			LOGGER.log(Level.INFO, "onStreamTermination called for "
					+ partitionKey.toString() + ", commit file.");
			commitFile();
			return BatchResponse.advanceAndGetNextBatch();
		} catch (Exception e) {
			return dstError("Fail to commit file. " + e.toString(), e);
		}
	}

	public synchronized void close() {
		this.dropTmpFile();
		clearOffsets();
	}

	// make upstream offset revert when offset error occurs
	protected BatchResponse offsetError(String errorMsg) {
		// clear lastOffset record, always consider offset in zookeeper true
		clearOffsets();
		dropTmpFile();
		LOGGER.log(Level.SEVERE, "offsetError: " + errorMsg);
		return BatchResponse.revertAndGetNextBatch();
	}

	/**
	 * drop current tmp file
	 */
	protected synchronized void dropTmpFile() {
		try {
			if (currentWriter != null) {
				currentWriter.close();
				hdfs.delete(currentTmpFile, false);
				LOGGER.log(Level.INFO, "Drop tmpFile " + currentTmpFile);
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Fail to drop tmp file " + currentTmpFile,
					e);
		} finally {
			try {
				progress.onDropFile();
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, e.toString(), e);
			}

			if (config.isLogErrorEvents() && currentErrorWriter != null) {
				try {
					currentErrorWriter.close();
				} catch (Exception e) {
					LOGGER.log(Level.SEVERE, e.toString(), e);
				}
			}

			cleanupFile();
		}
	}

	private void clearOffsets() {
		LOGGER.log(Level.INFO, "Clear offsets for " + partitionKey.toString());
		lastOffset = -1L;
		startOffset = 0L;
	}

	protected void openFile(Collection<JetstreamEvent> events)
			throws IOException {
		startOffset = lastOffset + 1;
		currentFolder = progress.onNewFile(events);
		currentTmpFile = config.getWorkingFolder()
				+ "/"
				+ currentFolder
				+ "/"
				+ fileNameResolver.getTmpFileName(partitionKey.getTopic(),
						partitionKey.getPartition(), startOffset);

		OutputStream stream = hdfs.createFile(currentTmpFile, true);
		currentWriter = writerFactory.createEventWriter(stream);

		// log start time after file created
		loadStartTime = System.currentTimeMillis();
	}

	// make upstream wait when data destination error occurs, such as hdfs & zk
	protected BatchResponse dstError(String errorMsg, Exception e) {
		LOGGER.log(Level.SEVERE, "dstError: " + errorMsg, e);

		revertOffsets();
		dropTmpFile();
		return BatchResponse.revertAndGetNextBatch().setWaitTimeInMs(
				config.getWaitForFsAvaliableInMs());
	}

	private void revertOffsets() {
		LOGGER.log(Level.INFO, "Revert offset from " + lastOffset + " to "
				+ (startOffset - 1) + " for " + partitionKey.toString());
		lastOffset = startOffset - 1;
	}

	protected synchronized void commitFile() throws Exception {
		// flush has been committed, no need to commit again(during
		// rebalance)
		if (currentWriter == null)
			return;

		LOGGER.log(Level.INFO, "Closing stream for file " + currentTmpFile
				+ " with " + eventCount + " events in total.");

		try {
			currentWriter.close();
			Thread.sleep(config.getWaitForFileCloseInMs());

			// copy this tmp file to a new dst file named with lastOffset
			String fileName = writeDstFile();

			loadEndTime = System.currentTimeMillis();
			Map<String, Object> stats = genFileStats();
			currentWriter.handleStats(stats);

			// handle progress on commit;
			progressCommit(fileName, stats);
		} catch (Exception e) {
			throw new Exception(
					"Fail to close the current writer and rename the tmp file to dest file. "
							+ e.toString(), e);
		} finally {
			if (config.isLogErrorEvents() && currentErrorWriter != null) {
				try {
					currentErrorWriter.close();
				} catch (Throwable ex) {
					LOGGER.log(Level.SEVERE, "Fail to close error writer. "
							+ ex.toString(), ex);
				}
			}

			cleanupFile();
		}

	}

	protected void progressCommit(String fileName, Map<String, Object> stats)
			throws Exception {
		boolean ret = true;
		Exception ex = null;
		try {
			ret = progress.onCommitFile(fileName, stats);
		} catch (Exception e) {
			ex = e;
		}
		if (!ret) {
			ex = new Exception(
					"Progress fail to move forward, delete the dest file "
							+ dstFilePath);
		}
		if (ex != null) {
			try {
				hdfs.delete(dstFilePath, false);
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, "Fail to delete dst file "
						+ dstFilePath, e);
			}
			throw ex;
		}
	}

	/**
	 * copy tmp file to a dst file
	 */
	protected String writeDstFile() throws Exception {
		String dstFolderPath = config.getOutputFolder() + "/" + currentFolder;
		hdfs.createFolder(dstFolderPath);
		String destFileName = fileNameResolver.getDestFileName(
				partitionKey.getTopic(), partitionKey.getPartition(),
				startOffset, lastOffset);
		dstFilePath = dstFolderPath + "/" + destFileName;
		hdfs.rename(currentTmpFile, dstFilePath);
		LOGGER.log(Level.INFO, "Tmp file " + currentTmpFile
				+ " is renamed to dest file " + dstFilePath);
		return destFileName;
	}

	protected void cleanupFile() {
		currentWriter = null;
		currentErrorWriter = null;
		lastDstFilePath = dstFilePath;
		currentTmpFile = null;
		dstFilePath = null;
		eventCount = 0L;
		errorCount = 0L;
		loadStartTime = Long.MAX_VALUE;
		loadEndTime = 0L;
	}

	protected Map<String, Object> genFileStats() {
		Map<String, Object> stats = new LinkedHashMap<String, Object>();
		stats.put("hostName", MiscUtil.getLocalHostName());
		stats.put("topic", partitionKey.getTopic());
		stats.put("partition", partitionKey.getPartition());
		stats.put("startOffset", startOffset);
		stats.put("endOffset", lastOffset);
		stats.put("eventCount", eventCount);
		stats.put("errorCount", errorCount);

		if (loadStartTime != Long.MAX_VALUE)
			stats.put("loadStartTime", loadStartTime);
		else
			stats.put("loadStartTime", 0L);
		stats.put("loadEndTime", loadEndTime);
		return stats;
	}

	protected void logErrorEvent(JetstreamEvent event) {
		if (!config.isLogErrorEvents()) {
			return;
		}

		if (currentErrorWriter == null) {
			openErrorFile();
		}

		if (currentErrorWriter != null) {
			try {
				currentErrorWriter.write(event);
			} catch (Exception ex) {
				LOGGER.log(Level.SEVERE,
						"Fail to log error event. " + ex.toString(), ex);
			}
		}
	}

	protected void openErrorFile() {
		try {
			String errorFile = config.getErrorFolder()
					+ "/error/"
					+ currentFolder
					+ "/"
					+ fileNameResolver.getTmpFileName(partitionKey.getTopic(),
							partitionKey.getPartition(), startOffset)
					+ config.getErrorFileSuffix();

			OutputStream stream = hdfs.createFile(errorFile, true);
			currentErrorWriter = writerFactory.createEventWriter(stream);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE,
					"Fail to create error file. " + e.toString());
		}
	}
}
