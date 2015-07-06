/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;
import com.ebay.jetstream.event.processor.hdfs.HdfsClient;
import com.ebay.jetstream.event.processor.hdfs.transformer.JsonEventTransformer;

/**
 * @author weifang
 * 
 */
public class SequenceEventWriter extends AbstractNamedBean implements
		InitializingBean, EventWriter {
	private static Logger LOGGER = Logger
			.getLogger(SequenceEventWriter.class.getName());

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
	public EventWriterInstance open(OutputStream outStream) {
		try {
			Class<?> keyClass = Class.forName(keyClassName);
			Class<?> valueClass = Class.forName(valueClassName);
			CompressionType cType = CompressionType.valueOf(compressionType);
			return new SequenceEventWriterInstance(outStream, keyClass,
					valueClass, cType);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	class SequenceEventWriterInstance implements EventWriterInstance {

		private SequenceFile.Writer writer;
		private EventTransformer<Object> keyTransformer;
		private EventTransformer<Object> valueTransformer;

		public SequenceEventWriterInstance(OutputStream stream,//
				Class<?> keyClass, //
				Class<?> valueClass,//
				CompressionType compressionType) {
			if (!(stream instanceof FSDataOutputStream)) {
				throw new RuntimeException(
						"OutputStream must be a FSDataOutputStream");
			}
			try {
				writer = SequenceFile.createWriter(hdfs.getHadoopConfig(),
						Writer.stream((FSDataOutputStream) stream),
						Writer.keyClass(keyClass),
						Writer.valueClass(valueClass),
						Writer.compression(compressionType));
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

}
