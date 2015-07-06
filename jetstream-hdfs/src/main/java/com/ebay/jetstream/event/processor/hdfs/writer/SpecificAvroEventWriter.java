/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;

/**
 * @author weifang
 * 
 */
public class SpecificAvroEventWriter<T extends SpecificRecord> extends
		AbstractAvroEventWriter {
	// inject
	private String className;
	private EventTransformer<T> transformer;

	public void setClassName(String className) {
		this.className = className;
	}

	public void setTransformer(EventTransformer<T> transformer) {
		this.transformer = transformer;
	}

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
	public EventWriterInstance open(OutputStream outStream) {
		return new SpecificAvroEventWriterInstance(outStream, getSchema(),
				getSpecificClass(), getCodec());
	}

	class SpecificAvroEventWriterInstance implements EventWriterInstance {
		private DataFileWriter<T> writer;

		public SpecificAvroEventWriterInstance(OutputStream outStream,
				Schema schema, Class<T> cls, CodecFactory codecFactory) {
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
		 * com.ebay.jetstream.event.processor.hdfs.EventWriter#write(com.ebay
		 * .jetstream .event.JetstreamEvent)
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
}
