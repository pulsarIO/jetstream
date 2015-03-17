/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.io.OutputStream;

/**
 * @author weifang
 * 
 */
public interface EventWriterFactory {
	EventWriter createEventWriter(OutputStream outStream);
}
