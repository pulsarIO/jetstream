/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;

/**
 * @author weifang
 * 
 */
public class GenericAvroEventWriter extends
		AbstractAvroEventWriter {
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
	public EventWriterInstance open(OutputStream outStream) {
		return new GenericAvroEventWriterInstance(outStream, getSchema(),
				getCodec());
	}

	class GenericAvroEventWriterInstance implements EventWriterInstance {
		private DataFileWriter<GenericRecord> writer;

		public GenericAvroEventWriterInstance(OutputStream outStream,
				Schema schema, CodecFactory codecFactory) {
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
		 * com.ebay.jetstream.event.processor.hdfs.EventWriter#write(com.ebay
		 * .jetstream .event.JetstreamEvent)
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
