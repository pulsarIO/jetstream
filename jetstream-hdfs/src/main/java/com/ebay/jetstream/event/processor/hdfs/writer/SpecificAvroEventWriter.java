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
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;

/**
 * @author weifang
 * 
 */
public class SpecificAvroEventWriter<T extends SpecificRecord> implements
		EventWriter {
	private DataFileWriter<T> writer;
	private EventTransformer<T> transformer;

	public SpecificAvroEventWriter(OutputStream outStream, Schema schema,
			Class<T> cls, CodecFactory codecFactory) {
		try {
			DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(cls);
			writer = new DataFileWriter<T>(datumWriter);
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
		T value;
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
