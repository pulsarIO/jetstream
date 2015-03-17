/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.writer;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.EventWriter;

/**
 * @author weifang
 * 
 */
public class TextEventWriter implements EventWriter {
	private EventTransformer<String> transformer;
	private PrintWriter writer;

	public TextEventWriter(OutputStream stream,
			EventTransformer<String> transformer) {
		this.transformer = transformer;
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
	public void handleStats(Map<String, Object> stats) {
	}

	@Override
	public void close() {
		if (writer != null) {
			writer.close();
		}
	}

}
