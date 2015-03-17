/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.OutputStream;

import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;
import com.ebay.jetstream.event.processor.hdfs.EventWriterFactory;
import com.ebay.jetstream.event.processor.hdfs.transformer.JsonEventTransformer;

/**
 * @author weifang
 * 
 */
public class TextEventWriterFactory extends AbstractNamedBean implements
		InitializingBean, EventWriterFactory {
	private EventTransformer<String> transformer;

	public void setTransformer(EventTransformer<String> transformer) {
		this.transformer = transformer;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (transformer == null) {
			transformer = new JsonEventTransformer();
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
		return new TextEventWriter(outStream, transformer);
	}

}
