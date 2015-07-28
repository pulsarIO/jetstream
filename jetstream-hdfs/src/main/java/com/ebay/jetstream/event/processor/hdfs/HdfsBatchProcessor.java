/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.common.OffsetOutOfRangeException;

import org.springframework.context.ApplicationEvent;

import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.BatchSource;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.kafka.PartitionLostException;
import com.ebay.jetstream.event.processor.hdfs.EventWriter.EventWriterInstance;
import com.ebay.jetstream.event.processor.hdfs.resolver.SystemTimeFolderResolver;
import com.ebay.jetstream.event.processor.hdfs.util.MiscUtil;
import com.ebay.jetstream.event.processor.hdfs.writer.SequenceEventWriter;
import com.ebay.jetstream.event.support.AbstractBatchEventProcessor;

/**
 * @author weifang
 * 
 */
public class HdfsBatchProcessor extends AbstractBatchEventProcessor {
	public static final String SUFFIX_TMP_FILE = ".tmp";
	public static final String OTHER_EVENT_TYPE = "other";

	private static Logger LOGGER = Logger.getLogger(HdfsBatchProcessor.class
			.getName());

	// injected
	protected HdfsBatchProcessorConfig config;
	protected HdfsClient hdfs;
	protected EventWriter defaultEventWriter;
	protected Map<String, EventWriter> eventWriters;
	protected EventWriter errorEventWriter;
	protected FolderResolver folderResolver;
	protected List<BatchListener> listeners;

	// internal
	protected Map<PartitionKey, PartitionProcessor> partitionProcMap = new ConcurrentHashMap<PartitionKey, PartitionProcessor>();

	public void setConfig(HdfsBatchProcessorConfig config) {
		this.config = config;
	}

	public void setHdfs(HdfsClient hdfs) {
		this.hdfs = hdfs;
	}

	public void setFolderResolver(FolderResolver folderResolver) {
		this.folderResolver = folderResolver;
	}

	public void setDefaultEventWriter(EventWriter defaultEventWriter) {
		this.defaultEventWriter = defaultEventWriter;
	}

	public void setEventWriters(Map<String, EventWriter> eventWriters) {
		this.eventWriters = eventWriters;
	}

	public void setErrorEventWriter(EventWriter errorEventWriter) {
		this.errorEventWriter = errorEventWriter;
	}

	public void setListeners(List<BatchListener> listeners) {
		this.listeners = listeners;
	}

	@Override
	public void shutDown() {
		LOGGER.info("Shutting down HdfsBatchProcessor.");

		for (PartitionProcessor writer : partitionProcMap.values()) {
			try {
				writer.close();
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		LOGGER.info("Shutdown HdfsBatchProcessor finished.");
	}

	@Override
	public void init() throws Exception {
		if ((eventWriters == null || eventWriters.isEmpty())
				&& defaultEventWriter == null) {
			throw new RuntimeException(
					"No Event Writers configured for this processor. At least one "
							+ "of eventWriters or defaultEventWriter has to be provided.");
		}

		if (errorEventWriter == null) {
			SequenceEventWriter seqFactory = new SequenceEventWriter();
			seqFactory.setHdfs(hdfs);
			seqFactory.afterPropertiesSet();
			errorEventWriter = seqFactory;
		}

		if (folderResolver == null) {
			folderResolver = new SystemTimeFolderResolver();
		}
	}

	@Override
	public BatchResponse onNextBatch(BatchSource source,
			Collection<JetstreamEvent> events) throws EventException {
		String topic = source.getTopic();
		int partition = (Integer) source.getPartition();
		long headOffset = source.getHeadOffset();

		PartitionKey key = new PartitionKey(topic, partition);
		PartitionProcessor writer = partitionProcMap.get(key);
		if (writer == null) {
			synchronized (partitionProcMap) {
				if (!partitionProcMap.containsKey(key)) {
					writer = createPartitionWriter(key);
					partitionProcMap.put(key, writer);
				}
			}
		}
		try {
			// count those dropped in logic and without exception
			AtomicLong dropped = new AtomicLong(0);
			super.incrementEventRecievedCounter(events.size());
			BatchResponse res = writer.writeBatch(headOffset, events, dropped);
			super.incrementEventDroppedCounter(dropped.get());
			super.incrementEventSentCounter(events.size() - dropped.get());
			return res;
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Error occurs when doBatch.", e);
			return BatchResponse.revertAndGetNextBatch();
		}
	}

	@Override
	public void onBatchProcessed(BatchSource source) {
	}

	@Override
	public BatchResponse onStreamTermination(BatchSource source)
			throws EventException {
		String topic = source.getTopic();
		int partition = (Integer) source.getPartition();

		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.log(Level.INFO, MessageFormat.format(
					"topic {0} partition {1} to be committed by upstream.",
					topic, partition));
		}

		PartitionKey key = new PartitionKey(topic, partition);
		PartitionProcessor writer = partitionProcMap.get(key);
		if (writer != null) {
			BatchResponse res = writer.handleStreamTermination();
			writer.close();
			partitionProcMap.remove(key);
			return res;
		} else {
			return BatchResponse.advanceAndGetNextBatch();
		}
	}

	@Override
	public BatchResponse onException(BatchSource source, Exception ex) {
		String topic = source.getTopic();
		int partition = (Integer) source.getPartition();
		PartitionKey key = new PartitionKey(topic, partition);

		LOGGER.log(Level.SEVERE, "Onexception for stream of " + key, ex);
		PartitionProcessor writer = partitionProcMap.get(key);
		if (writer != null) {
			if (ex instanceof PartitionLostException) {
				LOGGER.log(Level.SEVERE,
						"PartitionLost, drop the status of this partition.", ex);
				writer.close();
				partitionProcMap.remove(key);
			} else {
				return writer.handleException(ex);
			}
		}
		return BatchResponse.getNextBatch();
	}

	@Override
	public BatchResponse onIdle(BatchSource source) {
		String topic = source.getTopic();
		int partition = (Integer) source.getPartition();
		PartitionKey key = new PartitionKey(topic, partition);

		PartitionProcessor writer = partitionProcMap.get(key);
		if (writer != null) {
			return writer.handleIdle();
		}
		return BatchResponse.getNextBatch();
	}

	@Override
	public void pause() {
		// doesn't handle pause and resume, depends on the upstream
	}

	@Override
	protected void processApplicationEvent(ApplicationEvent event) {
		// TODO Hot deploy
	}

	@Override
	public void resume() {
		// doesn't handle pause and resume, depends on the upstream
	}

	protected PartitionProcessor createPartitionWriter(PartitionKey key) {
		return new PartitionProcessor(key);
	}

	class PartitionProcessor {
		protected final PartitionKey partitionKey;

		// internal
		protected String currentFolder;
		protected EventWriterInstance errorWriterInstance;
		protected long startOffset = -1L;
		protected long lastOffset = -1L;
		protected EventWriterWrapper defaultWriterInstance;
		protected Map<String, EventWriterWrapper> writerInstances = null;

		public PartitionProcessor(PartitionKey partitionKey) {
			this.partitionKey = partitionKey;
		}

		protected EventWriterInstance createEventWriterInstance(
				String eventType, OutputStream output) {
			EventWriter writer = eventWriters.get(eventType);
			if (writer == null) {
				writer = defaultEventWriter;
			}

			if (writer != null) {
				return writer.open(output);
			} else {
				throw new RuntimeException("No EventWriters found for "
						+ eventType + " , and no default EventWriter");
			}
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
				return offsetError(partitionKey.toString()
						+ ": events of offset " + (lastOffset + 1)
						+ " to offset " + (headOffset - 1) + " is lost.");
			}

			if (writerInstances == null) {
				try {
					startOffset = lastOffset + 1;
					currentFolder = folderResolver.getCurrentFolder(events);
					openFiles();
				} catch (Exception e) {
					return dstError("Fail to open file. " + e.toString(), e);
				}
			}

			long offset = headOffset - 1;
			for (BatchListener listener : listeners) {
				listener.onBatchBegin(partitionKey, headOffset);
			}
			try {
				for (JetstreamEvent event : events) {
					offset++;
					if (offset - lastOffset < 1) {
						droppedCounter.incrementAndGet();
						continue;
					}

					EventWriterWrapper wrapper = getEventWriter(event
							.getEventType());
					if (wrapper != null) {
						boolean rs = wrapper.getWriterInstance().write(event);
						if (!rs) {
							logErrorEvent(event);
							droppedCounter.incrementAndGet();
							for (BatchListener listener : listeners) {
								listener.onEventError(partitionKey,
										wrapper.getEventType(), event);
							}
							continue;
						}
					} else {
						droppedCounter.incrementAndGet();
					}

					for (BatchListener listener : listeners) {
						listener.onEventWritten(partitionKey,
								wrapper.getEventType(), event);
					}
				}

			} catch (Exception e) {
				return dstError(
						"Error occurs during writing events. " + e.toString(),
						e);
			}

			for (BatchListener listener : listeners) {
				listener.onBatchEnd(partitionKey, tailOffset);
			}

			lastOffset = tailOffset;
			if (folderResolver.shouldMoveToNext(events, currentFolder)) {
				try {
					commitFiles();
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
							+ partitionKey.toString()
							+ ", Try to commit file. ");
					// TODO
					// offset out of range after init could be a fatal error of
					// performance
					commitFiles();
					return BatchResponse.advanceAndGetNextBatch();
				} catch (Exception e) {
					return dstError("Fail to commit file. " + e.toString(), e);
				}
			}

			LOGGER.log(Level.INFO, "Unknown exception happended on "
					+ partitionKey.toString() + ". " + ex.toString(), ex);

			// unknow exception, anyway drop
			if (writerInstances != null) {
				dropTmpFiles();
				revertOffsets();
			}
			// TODO removed the lastDstFilePath logic. need to confirm.
			return BatchResponse.revertAndGetNextBatch();
		}

		public synchronized BatchResponse handleIdle() {
			try {
				LOGGER.log(Level.INFO,
						"onIdle called for " + partitionKey.toString()
								+ ", commit file. ");
				commitFiles();
				return BatchResponse.advanceAndGetNextBatch();
			} catch (Exception e) {
				return dstError("Fail to commit file. " + e.toString(), e);
			}
		}

		public synchronized BatchResponse handleStreamTermination() {
			try {
				LOGGER.log(Level.INFO, "onStreamTermination called for "
						+ partitionKey.toString() + ", commit file.");
				commitFiles();
				return BatchResponse.advanceAndGetNextBatch();
			} catch (Exception e) {
				return dstError("Fail to commit file. " + e.toString(), e);
			}
		}

		public synchronized void close() {
			this.dropTmpFiles();
			clearOffsets();
		}

		// make upstream offset revert when offset error occurs
		protected BatchResponse offsetError(String errorMsg) {
			// clear lastOffset record, always consider offset in zookeeper true
			clearOffsets();
			dropTmpFiles();
			LOGGER.log(Level.SEVERE, "offsetError: " + errorMsg);
			return BatchResponse.revertAndGetNextBatch();
		}

		/**
		 * drop current tmp file
		 */
		protected synchronized void dropTmpFiles() {

			if (writerInstances != null) {
				for (Entry<String, EventWriterWrapper> entry : writerInstances
						.entrySet()) {
					dropTmpFile(entry.getValue());
				}
			}

			if (defaultWriterInstance != null) {
				dropTmpFile(defaultWriterInstance);
			}

			try {
				for (BatchListener listener : listeners) {
					listener.onFilesDropped(partitionKey, currentFolder);
				}
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, e.toString(), e);
			}

			cleanup();
		}

		protected void dropTmpFile(EventWriterWrapper writerWrapper) {
			String filePath = null;
			try {
				writerWrapper.getWriterInstance().close();
				filePath = writerWrapper.getTmpFilePath();
				hdfs.delete(filePath, false);
				LOGGER.log(Level.INFO, "Drop tmpFile " + filePath);
			} catch (Exception e) {
				LOGGER.log(Level.SEVERE, "Fail to drop tmp file " + filePath, e);
			}
		}

		private void clearOffsets() {
			LOGGER.log(Level.INFO,
					"Clear offsets for " + partitionKey.toString());
			lastOffset = -1L;
			startOffset = 0L;
		}

		protected void openFiles() throws IOException {
			writerInstances = new HashMap<String, EventWriterWrapper>();

			String tmpFileName = getTmpFileName(partitionKey.getTopic(),
					partitionKey.getPartition(), startOffset);
			List<String> eventTypes = new ArrayList<String>();
			if (eventWriters == null || eventWriters.isEmpty()) {
				defaultWriterInstance = openFile(defaultEventWriter, null,
						tmpFileName);
				eventTypes.add(null);
			} else {
				for (Entry<String, EventWriter> entry : eventWriters.entrySet()) {
					String eventType = entry.getKey();
					EventWriter writer = entry.getValue();
					EventWriterWrapper wrapper = openFile(writer, eventType,
							tmpFileName);
					writerInstances.put(eventType, wrapper);
					eventTypes.add(eventType);
				}
				if (defaultEventWriter != null) {
					String defaultName = MiscUtil.getUniqueNameWithNumbers(
							eventWriters.keySet(), OTHER_EVENT_TYPE);
					defaultWriterInstance = openFile(defaultEventWriter,
							defaultName, tmpFileName);
					eventTypes.add(defaultName);
				}
			}

			for (BatchListener listener : listeners) {
				listener.onFilesCreated(partitionKey, startOffset,
						currentFolder, eventTypes, tmpFileName);
			}
		}

		protected EventWriterWrapper openFile(EventWriter writer,
				String eventType, String tmpFileName) throws IOException {
			String filePath = config.getWorkingFolder() + "/" + currentFolder;
			if (eventType != null) {
				filePath += "/" + eventType;
			}
			filePath += "/" + tmpFileName;
			OutputStream stream = hdfs.createFile(filePath, true);
			EventWriterWrapper ret = new EventWriterWrapper();
			ret.setEventType(eventType);
			ret.setTmpFilePath(filePath);
			ret.setWriterInstance(writer.open(stream));
			return ret;
		}

		protected EventWriterWrapper getEventWriter(String eventType) {
			if (writerInstances == null || writerInstances.isEmpty()) {
				return defaultWriterInstance;
			} else {
				EventWriterWrapper wrapper = writerInstances.get(eventType);
				if (wrapper == null) {
					return defaultWriterInstance;
				} else {
					return wrapper;
				}
			}
		}

		// make upstream wait when data destination error occurs, such as hdfs &
		// zk
		protected BatchResponse dstError(String errorMsg, Exception e) {
			LOGGER.log(Level.SEVERE, "dstError: " + errorMsg, e);

			revertOffsets();
			dropTmpFiles();
			return BatchResponse.revertAndGetNextBatch().setWaitTimeInMs(
					config.getWaitForFsAvaliableInMs());
		}

		private void revertOffsets() {
			LOGGER.log(Level.INFO, "Revert offset from " + lastOffset + " to "
					+ (startOffset - 1) + " for " + partitionKey.toString());
			lastOffset = startOffset - 1;
		}

		protected synchronized void commitFiles() throws Exception {
			try {
				String destFileName = getDestFileName(partitionKey.getTopic(),
						partitionKey.getPartition(), startOffset, lastOffset);

				if (writerInstances != null) {
					for (Entry<String, EventWriterWrapper> entry : writerInstances
							.entrySet()) {
						commitFile(entry.getValue(), destFileName);
					}
				}

				if (defaultWriterInstance != null) {
					commitFile(defaultWriterInstance, destFileName);
				}

				boolean ret = true;
				for (BatchListener listener : listeners) {
					ret = ret
							&& listener.onFilesCommited(partitionKey,
									startOffset, lastOffset, currentFolder,
									destFileName);
				}

				if (!ret) {
					throw new Exception(
							"One or more listeners returned false. Cancel file committing.");
				}
			} catch (Exception e) {
				if (writerInstances != null) {
					for (EventWriterWrapper wrapper : writerInstances.values()) {
						checkAndDelete(wrapper.getDstFilePath());
					}
				}
				if (defaultWriterInstance != null) {
					checkAndDelete(defaultWriterInstance.getDstFilePath());
				}
				throw e;
			} finally {
				cleanup();
			}
		}

		protected void checkAndDelete(String dstFilePath) {
			try {
				if (hdfs.exist(dstFilePath)) {
					hdfs.delete(dstFilePath, false);
				}
			} catch (Exception e2) {
				LOGGER.log(Level.SEVERE, e2.getMessage(), e2);
			}
		}

		protected void commitFile(EventWriterWrapper wrapper, String destName)
				throws IOException {
			wrapper.getWriterInstance().close();
			String dstFolderPath = config.getOutputFolder() + "/"
					+ currentFolder;
			String eventType = wrapper.getEventType();
			if (eventType != null) {
				dstFolderPath += "/" + eventType;
			}
			hdfs.createFolder(dstFolderPath);
			String dstFilePath = dstFolderPath + "/" + destName;
			String tmpFilePath = wrapper.getTmpFilePath();
			hdfs.rename(wrapper.getTmpFilePath(), dstFilePath);
			LOGGER.log(Level.INFO, "Tmp file " + tmpFilePath
					+ " is renamed to dest file " + dstFilePath);
			wrapper.setDstFilePath(dstFilePath);
		}

		protected void cleanup() {
			if (config.isLogErrorEvents() && errorWriterInstance != null) {
				try {
					errorWriterInstance.close();
				} catch (Throwable ex) {
					LOGGER.log(Level.SEVERE, "Fail to close error writer. "
							+ ex.toString(), ex);
				}
			}
			writerInstances = null;
			errorWriterInstance = null;
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

	static class EventWriterWrapper {
		private String eventType;
		private EventWriterInstance writerInstance;
		private String tmpFilePath;
		private String dstFilePath;

		public String getEventType() {
			return eventType;
		}

		public void setEventType(String eventType) {
			this.eventType = eventType;
		}

		public EventWriterInstance getWriterInstance() {
			return writerInstance;
		}

		public void setWriterInstance(EventWriterInstance writerInstance) {
			this.writerInstance = writerInstance;
		}

		public String getTmpFilePath() {
			return tmpFilePath;
		}

		public void setTmpFilePath(String tmpFilePath) {
			this.tmpFilePath = tmpFilePath;
		}

		public String getDstFilePath() {
			return dstFilePath;
		}

		public void setDstFilePath(String dstFilePath) {
			this.dstFilePath = dstFilePath;
		}
	}

}
