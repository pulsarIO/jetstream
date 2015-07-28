/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import com.ebay.jetstream.event.JetstreamEvent;

/**
 * @author weifang
 *
 */
public class TestSpecificAvroTransformer implements
		EventTransformer<TestRecord> {

	@Override
	public TestRecord transform(JetstreamEvent event) {
		TestRecord record = new TestRecord();
		record.setKey1((String) event.get("key1"));
		record.setKey2((String) event.get("key2"));
		return record;
	}

}
