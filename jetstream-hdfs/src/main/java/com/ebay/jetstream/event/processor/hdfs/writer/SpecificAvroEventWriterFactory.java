/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.OutputStream;
import java.lang.reflect.Method;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import com.ebay.jetstream.event.processor.hdfs.EventWriter;

/**
 * @author weifang
 * 
 */
public class SpecificAvroEventWriterFactory<T extends SpecificRecord> extends
		AbstractAvroEventWriterFactory {

	// inject
	private String className;

	@Override
	protected Schema loadSchema() throws Exception {
		Schema schema = super.loadSchema();
		if (schema == null) {
			Class<T> cls = getSpecificClass();
			Method m = cls.getDeclaredMethod("getClassSchema", new Class<?>[0]);
			schema = (Schema) m.invoke(null, new Object[0]);
		}
		return schema;
	}

	@SuppressWarnings("unchecked")
	protected Class<T> getSpecificClass() {
		try {
			return (Class<T>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
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
		return new SpecificAvroEventWriter<T>(outStream, getSchema(),
				getSpecificClass(), getCodec());
	}

}
