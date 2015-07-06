/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import com.ebay.jetstream.event.JetstreamEvent;

/**
 * @author weifang
 * 
 */
public interface EventTransformer<T> {
	public T transform(JetstreamEvent event);
}
