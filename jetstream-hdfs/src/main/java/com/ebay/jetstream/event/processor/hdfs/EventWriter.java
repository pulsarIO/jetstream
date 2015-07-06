/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.io.OutputStream;

import com.ebay.jetstream.event.JetstreamEvent;

/**
 * @author weifang
 * 
 */
public interface EventWriter {
	EventWriterInstance open(OutputStream output);

	interface EventWriterInstance {
		boolean write(JetstreamEvent event);

		void close();
	}
}
