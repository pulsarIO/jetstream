/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;

import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.BatchSource;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.kafka.PartitionLostException;
import com.ebay.jetstream.event.processor.hdfs.writer.SequenceEventWriterFactory;
import com.ebay.jetstream.event.support.AbstractBatchEventProcessor;
import com.ebay.jetstream.messaging.MessageServiceTimer;

/**
 * @author weifang
 * 
 */
public class HdfsBatchProcessor extends AbstractBatchEventProcessor implements
		ApplicationContextAware, StatsAggregator {
	private static Logger LOGGER = Logger.getLogger(HdfsBatchProcessor.class
			.getName());

	// injected
	protected HdfsBatchProcessorConfig config;
	protected HdfsClient hdfs;
	protected ProgressController progressController;
	protected EventWriterFactory writerFactory;
	protected EventWriterFactory errorWriterFactory;
	protected FileNameResolver fileNameResolver;
	protected ApplicationContext appContext;

	// internal
	protected Map<PartitionKey, PartitionWriter> partitionWriterMap = new ConcurrentHashMap<PartitionKey, PartitionWriter>();
	protected SuccessTask successTask;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.appContext = applicationContext;
	}

	public void setConfig(HdfsBatchProcessorConfig config) {
		this.config = config;
	}

	public void setHdfs(HdfsClient hdfs) {
		this.hdfs = hdfs;
	}

	public void setProgressController(ProgressController progressController) {
		this.progressController = progressController;
	}

	public void setWriterFactory(EventWriterFactory writerFactory) {
		this.writerFactory = writerFactory;
	}

	public void setErrorWriterFactory(EventWriterFactory errorWriterFactory) {
		this.errorWriterFactory = errorWriterFactory;
	}

	public void setFileNameResolver(FileNameResolver fileNameResolver) {
		this.fileNameResolver = fileNameResolver;
	}

	@Override
	public void shutDown() {
		LOGGER.info("Shutting down HdfsBatchProcessor.");
		if (successTask != null) {
			successTask.cancel();
		}
		for (PartitionWriter writer : partitionWriterMap.values()) {
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
		if (errorWriterFactory == null) {
			SequenceEventWriterFactory seqFactory = new SequenceEventWriterFactory();
			seqFactory.setHdfs(hdfs);
			seqFactory.afterPropertiesSet();
		}

		if (fileNameResolver == null) {
			fileNameResolver = new DefaultFileNameResolver();
		}

		successTask = new SuccessTask();
		long interval = config.getSuccessCheckInterval();
		MessageServiceTimer.sInstance().schedulePeriodicTask(successTask,
				interval, interval);

		progressController.setOutputFolder(config.getOutputFolder());
	}

	@Override
	public BatchResponse onNextBatch(BatchSource source,
			Collection<JetstreamEvent> events) throws EventException {
		String topic = source.getTopic();
		int partition = (Integer) source.getPartition();
		long headOffset = source.getHeadOffset();

		PartitionKey key = new PartitionKey(topic, partition);
		PartitionWriter writer = partitionWriterMap.get(key);
		if (writer == null) {
			synchronized (partitionWriterMap) {
				if (!partitionWriterMap.containsKey(key)) {
					writer = createPartitionWriter(key);
					partitionWriterMap.put(key, writer);
				}
			}
		}
		try {
			// count those dropped in logic and without exception
			AtomicLong dropped = new AtomicLong(0);
			super.incrementEventRecievedCounter(events.size());
			BatchResponse res = writer.doBatch(headOffset, events, dropped);
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
		PartitionWriter writer = partitionWriterMap.get(key);
		if (writer != null) {
			BatchResponse res = writer.doStreamTermination();
			writer.close();
			partitionWriterMap.remove(key);
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
		PartitionWriter writer = partitionWriterMap.get(key);
		if (writer != null) {
			if (ex instanceof PartitionLostException) {
				LOGGER.log(Level.SEVERE,
						"PartitionLost, drop the status of this partition.", ex);
				writer.close();
				partitionWriterMap.remove(key);
			} else {
				return writer.doException(ex);
			}
		}
		return BatchResponse.getNextBatch();
	}

	@Override
	public BatchResponse onIdle(BatchSource source) {
		String topic = source.getTopic();
		int partition = (Integer) source.getPartition();
		PartitionKey key = new PartitionKey(topic, partition);

		PartitionWriter writer = partitionWriterMap.get(key);
		if (writer != null) {
			return writer.doIdle();
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

	protected PartitionWriter createPartitionWriter(PartitionKey key) {
		PartitionProgressController progress = progressController
				.createPartitionProgressController(key);
		return new PartitionWriter(config,//
				key, //
				hdfs, //
				progress, //
				writerFactory, //
				errorWriterFactory, //
				fileNameResolver);
	}

	class SuccessTask extends TimerTask {

		@Override
		public void run() {
			try {
				List<String> folders = progressController
						.getSuccessCheckFolders();
				for (String folder : folders) {
					try {
						if (progressController.isSuccess(folder)) {
							Map<String, Map<String, Object>> fileStats = progressController
									.getFileStats(folder);
							if (fileStats != null) {
								Map<String, Object> aggregated = new LinkedHashMap<String, Object>();
								Map<String, StatsAggregator> aggregators = appContext
										.getBeansOfType(StatsAggregator.class);
								for (StatsAggregator agg : aggregators.values()) {
									agg.aggregateStats(fileStats, aggregated);
								}
								progressController.markSuccess(folder,
										fileStats, aggregated);
								hdfs.delete(config.getWorkingFolder() + "/"
										+ folder, true);
							}
						}
					} catch (Exception e) {
						LOGGER.log(Level.SEVERE,
								"Fail to check success for folder " + folder, e);
					}
				}
			} catch (Throwable th) {
				LOGGER.log(Level.SEVERE, th.toString(), th);
			}
		}
	}

	@Override
	public void aggregateStats(Map<String, Map<String, Object>> fileStats,
			Map<String, Object> aggregatedStats) {
		long eventCount = 0;
		long errorCount = 0;
		long firstLoadStartTime = Long.MAX_VALUE;
		long lastLoadEndTime = 0;
		Number n = null;
		int fileCount = 0;
		for (Entry<String, Map<String, Object>> entry : fileStats.entrySet()) {
			Map<String, Object> stats = entry.getValue();
			fileCount++;
			n = (Number) stats.get("eventCount");
			if (n != null) {
				eventCount += n.longValue();
			}
			n = (Number) stats.get("errorCount");
			if (n != null) {
				errorCount += n.longValue();
			}
			n = (Number) stats.get("loadStartTime");
			if (n != null && firstLoadStartTime > n.longValue()) {
				firstLoadStartTime = n.longValue();
			}
			n = (Number) stats.get("loadEndTime");
			if (n != null && lastLoadEndTime < n.longValue()) {
				lastLoadEndTime = n.longValue();
			}
		}

		aggregatedStats.put("fileCount", fileCount);
		aggregatedStats.put("totalEventCount", eventCount);
		aggregatedStats.put("totalErrorCount", errorCount);
		aggregatedStats.put("firstLoadStartTime", firstLoadStartTime);
		aggregatedStats.put("lastLoadEndTime", lastLoadEndTime);
	}
}
