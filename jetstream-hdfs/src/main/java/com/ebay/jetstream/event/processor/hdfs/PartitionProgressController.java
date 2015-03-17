/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.util.Collection;
import java.util.Map;

import com.ebay.jetstream.event.JetstreamEvent;

/**
 * @author weifang
 * 
 */
public interface PartitionProgressController {
	String onNewFile(Collection<JetstreamEvent> fristBatch);

	void onStartBatch();

	void onNextEvent(JetstreamEvent event);

	boolean onEndBatch();

	boolean onCommitFile(String fileName, Map<String, Object> stats);

	void onDropFile();
}
