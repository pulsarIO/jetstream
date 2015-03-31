/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.context.ApplicationEvent;

import com.ebay.jetstream.event.BatchResponse;
import com.ebay.jetstream.event.BatchSource;
import com.ebay.jetstream.event.EventException;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.kafka.PartitionLostException;
import com.ebay.jetstream.event.processor.hdfs.resolver.SystemTimeFolderResolver;
import com.ebay.jetstream.event.processor.hdfs.writer.SequenceEventWriter;
import com.ebay.jetstream.event.support.AbstractBatchEventProcessor;

/**
 * @author weifang
 * 
 */
public class HdfsBatchProcessor extends AbstractBatchEventProcessor {
	private static Logger LOGGER = Logger.getLogger(HdfsBatchProcessor.class
			.getName());

	// injected
	protected HdfsBatchProcessorConfig config;
	protected HdfsClient hdfs;
	protected EventWriter writerFactory;
	protected EventWriter errorWriterFactory;
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

	public void setWriterFactory(EventWriter writerFactory) {
		this.writerFactory = writerFactory;
	}

	public void setErrorWriterFactory(EventWriter errorWriterFactory) {
		this.errorWriterFactory = errorWriterFactory;
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
		if (errorWriterFactory == null) {
			SequenceEventWriter seqFactory = new SequenceEventWriter();
			seqFactory.setHdfs(hdfs);
			seqFactory.afterPropertiesSet();
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
		return new PartitionProcessor(config,//
				key, //
				hdfs, //
				writerFactory, //
				errorWriterFactory, //
				folderResolver, //
				listeners);
	}
}
