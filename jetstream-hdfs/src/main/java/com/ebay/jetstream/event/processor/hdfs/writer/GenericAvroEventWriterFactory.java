/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.OutputStream;

import com.ebay.jetstream.event.processor.hdfs.EventWriter;

/**
 * @author weifang
 * 
 */
public class GenericAvroEventWriterFactory extends
		AbstractAvroEventWriterFactory {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.EventWriterFactory#createEventWriter
	 * (java.io.OutputStream)
	 */
	@Override
	public EventWriter createEventWriter(OutputStream outStream) {
		return new GenericAvroEventWriter(outStream, getSchema(), getCodec());
	}

}
