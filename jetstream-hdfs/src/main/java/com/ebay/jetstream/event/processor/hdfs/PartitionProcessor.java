/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.common.OffsetOutOfRangeException;

import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventWriter.EventWriterInstance;
import com.ebay.jetstream.event.processor.hdfs.util.MiscUtil;

/**
 * @author weifang
 * 
 */
public class PartitionProcessor {
	public static final String SUFFIX_TMP_FILE = ".tmp";
	private static final Logger LOGGER = Logger
			.getLogger(PartitionProcessor.class.getName());

	// construct
	protected final HdfsBatchProcessorConfig config;
	protected final PartitionKey partitionKey;
	protected final HdfsClient hdfs;
	protected final EventWriter eventWriter;
	protected final EventWriter errorEventWriter;
	protected final FolderResolver folderResolver;
	protected final List<BatchListener> listeners;

	// internal
	protected String currentFolder;

	protected EventWriterInstance writerInstance;
	protected EventWriterInstance errorWriterInstance;
	protected long startOffset = -1L;
	protected long lastOffset = -1L;
	protected String currentTmpFile;
	protected String dstFilePath;
	protected String lastDstFilePath;

	public PartitionProcessor(HdfsBatchProcessorConfig config,//
			PartitionKey partitionKey, //
			HdfsClient hdfs, //
			EventWriter eventWriter, //
			EventWriter errorEventWriter, //
			FolderResolver folderResolver, //
			List<BatchListener> listeners) {
		this.config = config;
		this.partitionKey = partitionKey;
		this.hdfs = hdfs;
		this.eventWriter = eventWriter;
		this.errorEventWriter = errorEventWriter;
		this.folderResolver = folderResolver;
		this.listeners = listeners;
	}

	public synchronized BatchResponse writeBatch(final long headOffset,
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

		if (writerInstance == null) {
			try {
				openFile(events);
			} catch (Exception e) {
				return dstError("Fail to open file. " + e.toString(), e);
			}
		}

		int writtenCount = 0;
		int errorCount = 0;
		long offset = headOffset - 1;
		try {
			for (JetstreamEvent event : events) {
				offset++;
				if (offset - lastOffset < 1) {
					droppedCounter.incrementAndGet();
					continue;
				}

				boolean rs = writerInstance.write(event);
				if (!rs) {
					logErrorEvent(event);
					droppedCounter.incrementAndGet();
					errorCount++;
					continue;
				}

				writtenCount++;
			}

		} catch (Exception e) {
			return dstError(
					"Error occurs during writing events. " + e.toString(), e);
		}

		lastOffset = tailOffset;
		for (BatchListener listener : listeners) {
			listener.onBatchCompleted(partitionKey, writtenCount, errorCount,
					headOffset, events);
		}

		if (folderResolver.shouldMoveToNext(events, currentFolder)) {
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

	public synchronized BatchResponse handleException(Exception ex) {
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
		if (writerInstance != null) {
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

	public synchronized BatchResponse handleIdle() {
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

	public synchronized BatchResponse handleStreamTermination() {
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
			if (writerInstance != null) {
				writerInstance.close();
				hdfs.delete(currentTmpFile, false);
				LOGGER.log(Level.INFO, "Drop tmpFile " + currentTmpFile);
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Fail to drop tmp file " + currentTmpFile,
					e);
		} finally {
			try {
				for (BatchListener listener : listeners) {
					listener.onFileDropped(partitionKey, currentFolder,
							currentTmpFile);
				}
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, e.toString(), e);
			}

			if (config.isLogErrorEvents() && errorWriterInstance != null) {
				try {
					errorWriterInstance.close();
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
		currentFolder = folderResolver.getCurrentFolder(events);
		currentTmpFile = config.getWorkingFolder()
				+ "/"
				+ currentFolder
				+ "/"
				+ getTmpFileName(partitionKey.getTopic(),
						partitionKey.getPartition(), startOffset);

		OutputStream stream = hdfs.createFile(currentTmpFile, true);
		writerInstance = eventWriter.open(stream);
		for (BatchListener listener : listeners) {
			listener.onFileCreated(partitionKey, startOffset, currentFolder,
					currentTmpFile);
		}
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
		if (writerInstance == null)
			return;

		LOGGER.log(Level.INFO, "Closing stream for file " + currentTmpFile);

		try {
			writerInstance.close();
			Thread.sleep(config.getWaitForFileCloseInMs());

			// copy this tmp file to a new dst file named with lastOffset
			String fileName = writeDstFile();
			listenOnCommit(fileName);
		} catch (Exception e) {
			throw new Exception(
					"Fail to close the current writer and rename the tmp file to dest file. "
							+ e.toString(), e);
		} finally {
			if (config.isLogErrorEvents() && errorWriterInstance != null) {
				try {
					errorWriterInstance.close();
				} catch (Throwable ex) {
					LOGGER.log(Level.SEVERE, "Fail to close error writer. "
							+ ex.toString(), ex);
				}
			}

			cleanupFile();
		}

	}

	protected void listenOnCommit(String fileName) throws Exception {
		boolean ret = true;
		Exception ex = null;
		try {
			for (BatchListener listener : listeners) {
				ret = ret
						&& listener.onFileCommited(partitionKey, startOffset,
								lastOffset, currentFolder, fileName);
			}
		} catch (Exception e) {
			ex = e;
		}
		if (!ret) {
			ex = new Exception(
					"One or more listeners returned false. Cancel file committing.");
		}
		if (ex != null) {
			try {
				if (hdfs.exist(dstFilePath)) {
					hdfs.delete(dstFilePath, false);
				}
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
		String destFileName = getDestFileName(partitionKey.getTopic(),
				partitionKey.getPartition(), startOffset, lastOffset);
		dstFilePath = dstFolderPath + "/" + destFileName;
		hdfs.rename(currentTmpFile, dstFilePath);
		LOGGER.log(Level.INFO, "Tmp file " + currentTmpFile
				+ " is renamed to dest file " + dstFilePath);
		return destFileName;
	}

	protected void cleanupFile() {
		writerInstance = null;
		errorWriterInstance = null;
		lastDstFilePath = dstFilePath;
		currentTmpFile = null;
		dstFilePath = null;
	}

	protected void logErrorEvent(JetstreamEvent event) {
		if (!config.isLogErrorEvents()) {
			return;
		}

		if (errorWriterInstance == null) {
			openErrorFile();
		}

		if (errorWriterInstance != null) {
			try {
				errorWriterInstance.write(event);
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
					+ getTmpFileName(partitionKey.getTopic(),
							partitionKey.getPartition(), startOffset)
					+ config.getErrorFileSuffix();

			OutputStream stream = hdfs.createFile(errorFile, true);
			errorWriterInstance = errorEventWriter.open(stream);
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE,
					"Fail to create error file. " + e.toString());
		}
	}

	protected String getTmpFileName(String topic, int partition,
			long startOffset) {
		String fileNamePrefix = config.getFileNamePrefix();
		StringBuilder sb = new StringBuilder();
		if (fileNamePrefix != null && !fileNamePrefix.isEmpty()) {
			sb.append(fileNamePrefix).append("-");
		}
		return sb.append(topic).append("-").append(partition).append("-")
				.append(startOffset).append(SUFFIX_TMP_FILE).toString();
	}

	protected String getDestFileName(String topic, int partition,
			long startOffset, long endOffset) {
		String fileNamePrefix = config.getFileNamePrefix();
		String fileNameSuffix = config.getFileNameSuffix();
		StringBuilder sb = new StringBuilder();
		if (fileNamePrefix != null && !fileNamePrefix.isEmpty()) {
			sb.append(fileNamePrefix).append("-");
		}
		return sb.append(topic).append("-").append(partition).append("-")
				.append(startOffset).append("-").append(endOffset)
				.append(fileNameSuffix).toString();
	}
}
