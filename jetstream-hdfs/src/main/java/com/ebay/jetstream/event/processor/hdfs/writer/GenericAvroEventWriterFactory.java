/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.OutputStream;

import org.apache.avro.generic.GenericRecord;

import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;

/**
 * @author weifang
 * 
 */
public class GenericAvroEventWriterFactory extends
		AbstractAvroEventWriterFactory {
	// injected
	private EventTransformer<GenericRecord> transformer;

	public void setTransformer(EventTransformer<GenericRecord> transformer) {
		this.transformer = transformer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.EventWriterFactory#createEventWriter
	 * (java.io.OutputStream)
	 */
	@Override
	public EventWriter createEventWriter(OutputStream outStream) {
		return new GenericAvroEventWriter(outStream, transformer, getSchema(),
				getCodec());
	}

}
