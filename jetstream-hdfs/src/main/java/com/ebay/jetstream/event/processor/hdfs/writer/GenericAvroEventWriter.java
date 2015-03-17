/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;

/**
 * @author weifang
 * 
 */
public class GenericAvroEventWriter implements EventWriter {
	private DataFileWriter<GenericRecord> writer;
	private EventTransformer<GenericRecord> transformer;

	public GenericAvroEventWriter(OutputStream outStream,
			EventTransformer<GenericRecord> transformer, Schema schema,
			CodecFactory codecFactory) {
		this.transformer = transformer;
		try {
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
					schema);
			writer = new DataFileWriter<GenericRecord>(datumWriter);
			writer.setCodec(codecFactory);
			writer.create(schema, outStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.EventWriter#write(com.ebay.jetstream
	 * .event.JetstreamEvent)
	 */
	@Override
	public boolean write(JetstreamEvent event) {
		GenericRecord value;
		try {
			value = transformer.transform(event);
		} catch (Exception e) {
			return false;
		}
		try {
			writer.append(value);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ebay.jetstream.event.processor.hdfs.EventWriter#handleStats(java.
	 * util.Map)
	 */
	@Override
	public void handleStats(Map<String, Object> stats) {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.processor.hdfs.EventWriter#close()
	 */
	@Override
	public void close() {
		try {
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
