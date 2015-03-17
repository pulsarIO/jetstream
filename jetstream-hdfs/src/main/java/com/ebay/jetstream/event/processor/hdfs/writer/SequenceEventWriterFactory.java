/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.OutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;
import com.ebay.jetstream.event.processor.hdfs.EventWriterFactory;
import com.ebay.jetstream.event.processor.hdfs.HdfsClient;
import com.ebay.jetstream.event.processor.hdfs.transformer.JsonEventTransformer;

/**
 * @author weifang
 * 
 */
public class SequenceEventWriterFactory extends AbstractNamedBean implements
		InitializingBean, EventWriterFactory {
	// inject
	private HdfsClient hdfs;
	private String keyClassName;
	private String valueClassName;
	private EventTransformer<Object> keyTransformer;
	private EventTransformer<Object> valueTransformer;
	private String compressionType;

	public void setHdfs(HdfsClient hdfs) {
		this.hdfs = hdfs;
	}

	public void setKeyClassName(String keyClassName) {
		this.keyClassName = keyClassName;
	}

	public void setValueClassName(String valueClassName) {
		this.valueClassName = valueClassName;
	}

	public void setKeyTransformer(EventTransformer<Object> keyTransformer) {
		this.keyTransformer = keyTransformer;
	}

	public void setValueTransformer(EventTransformer<Object> valueTransformer) {
		this.valueTransformer = valueTransformer;
	}

	public void setCompressionType(String compressionType) {
		this.compressionType = compressionType;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (keyClassName == null) {
			keyClassName = NullWritable.class.getName();
		}
		if (valueClassName == null) {
			valueClassName = Text.class.getName();
		}
		if (keyTransformer == null) {
			keyTransformer = new EventTransformer<Object>() {
				@Override
				public Object transform(JetstreamEvent event) {
					return NullWritable.get();
				}
			};
		}
		if (valueTransformer == null) {
			valueTransformer = new EventTransformer<Object>() {
				JsonEventTransformer jsonTran = new JsonEventTransformer();

				@Override
				public Object transform(JetstreamEvent event) {
					String json = jsonTran.transform(event);
					return new Text(json);
				}
			};
		}
		if (compressionType == null) {
			compressionType = CompressionType.NONE.name();
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
		try {
			Class<?> keyClass = Class.forName(keyClassName);
			Class<?> valueClass = Class.forName(valueClassName);
			CompressionType cType = CompressionType.valueOf(compressionType);
			return new SequenceEventWriter(hdfs, outStream, keyClass,
					valueClass, keyTransformer, valueTransformer, cType);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

}
