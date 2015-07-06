/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.OutputStream;
import java.io.PrintWriter;

import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;
import com.ebay.jetstream.event.processor.hdfs.transformer.JsonEventTransformer;

/**
 * @author weifang
 * 
 */
public class TextEventWriter extends AbstractNamedBean implements
		InitializingBean, EventWriter {
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
	public EventWriterInstance open(OutputStream outStream) {
		return new TextEventWriterInstance(outStream);
	}

	class TextEventWriterInstance implements EventWriterInstance {
		private PrintWriter writer;

		public TextEventWriterInstance(OutputStream stream) {
			this.writer = new PrintWriter(stream);
		}

		@Override
		public boolean write(JetstreamEvent event) {
			String str = null;
			try {
				str = transformer.transform(event);
			} catch (Exception e) {
				return false;
			}
			writer.println(str);
			return true;
		}

		@Override
		public void close() {
			if (writer != null) {
				writer.close();
			}
		}

	}
}
