/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.processor.hdfs.EventWriterFactory;

/**
 * @author weifang
 * 
 */
public abstract class AbstractAvroEventWriterFactory extends AbstractNamedBean
		implements InitializingBean, EventWriterFactory {
	private static Logger LOGGER = Logger.getLogger(SequenceEventWriter.class
			.getName());

	public static final String CODEC_DEFLATE = "deflate";
	public static final String CODEC_SNAPPY = "snappy";
	public static final String CODEC_BZIP2 = "bzip2";
	public static final String CODEC_XY = "xy";

	// inject
	private String schemaContent;
	private String schemaLocation;
	private String codec;
	private int codecLevel;

	// internal
	private Schema schema;

	public void setSchemaLocation(String schemaLocation) {
		this.schemaLocation = schemaLocation;
	}

	public void setCodec(String codec) {
		this.codec = codec;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (codec == null) {
			codec = CODEC_DEFLATE;
			codecLevel = CodecFactory.DEFAULT_DEFLATE_LEVEL;
		}

		schema = loadSchema();
		if (schema == null) {
			throw new Exception(
					"Failed to load the avro schema. Schema can't be empty.");
		}
	}

	public Schema getSchema() {
		return schema;
	}

	protected Schema loadSchema() throws Exception {
		Schema schema = null;
		if (schemaContent != null) {
			schema = new Schema.Parser().parse(schemaContent);
		} else if (schemaLocation != null) {
			InputStream is = null;
			try {
				is = GenericAvroEventWriter.class
						.getResourceAsStream(schemaLocation);
				schema = new Schema.Parser().parse(is);
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (Exception e) {
						LOGGER.log(Level.SEVERE, e.toString(), e);
					}
				}
			}
		}

		return schema;
	}

	protected CodecFactory getCodec() {
		if (CODEC_DEFLATE.equalsIgnoreCase(codec)) {
			return CodecFactory.deflateCodec(codecLevel);
		} else if (CODEC_SNAPPY.equalsIgnoreCase(codec)) {
			return CodecFactory.snappyCodec();
		} else if (CODEC_BZIP2.equalsIgnoreCase(codec)) {
			return CodecFactory.bzip2Codec();
		} else if (CODEC_XY.equalsIgnoreCase(codec)) {
			return CodecFactory.xzCodec(codecLevel);
		} else {
			return CodecFactory.nullCodec();
		}
	}

}
