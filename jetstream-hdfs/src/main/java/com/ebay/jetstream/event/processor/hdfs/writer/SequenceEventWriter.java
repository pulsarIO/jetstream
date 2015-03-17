/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;
import com.ebay.jetstream.event.processor.hdfs.HdfsClient;

/**
 * @author weifang
 * 
 */
public class SequenceEventWriter implements EventWriter {
	private static Logger LOGGER = Logger.getLogger(SequenceEventWriter.class
			.getName());

	private SequenceFile.Writer writer;
	private EventTransformer<Object> keyTransformer;
	private EventTransformer<Object> valueTransformer;

	public SequenceEventWriter(HdfsClient hdfs, //
			OutputStream stream,//
			Class<?> keyClass, //
			Class<?> valueClass,//
			EventTransformer<Object> keyTransformer,//
			EventTransformer<Object> valueTransformer,//
			CompressionType compressionType) {
		if (!(stream instanceof FSDataOutputStream)) {
			throw new RuntimeException(
					"OutputStream must be a FSDataOutputStream");
		}
		try {
			writer = SequenceFile.createWriter(hdfs.getHadoopConfig(),
					Writer.stream((FSDataOutputStream) stream),
					Writer.keyClass(keyClass), Writer.valueClass(valueClass),
					Writer.compression(compressionType));
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
		Object key;
		Object value;
		try {
			key = keyTransformer.transform(event);
			value = valueTransformer.transform(event);
		} catch (Exception e) {
			return false;
		}
		try {
			writer.append(key, value);
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
		if (writer != null) {
			try {
				writer.hflush();
				writer.hsync();
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, e.toString(), e);
			} finally {
				try {
					writer.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

}
