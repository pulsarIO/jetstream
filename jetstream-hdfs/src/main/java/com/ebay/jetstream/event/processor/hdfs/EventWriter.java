/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.util.Map;

import com.ebay.jetstream.event.JetstreamEvent;

/**
 * @author weifang
 * 
 */
public interface EventWriter {
	boolean write(JetstreamEvent event);

	void handleStats(Map<String, Object> stats);

	void close();
}
