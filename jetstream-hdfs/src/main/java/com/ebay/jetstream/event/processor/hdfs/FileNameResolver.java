/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

/**
 * @author weifang
 * 
 */
public interface FileNameResolver {
	String getTmpFileName(String topic, int partition, long startOffset);
	
	String getDestFileName(String topic, int partition, long startOffset,
			long endOffset);
}
