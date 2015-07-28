/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.writer.GenericAvroEventTransformer;

/**
 * @author weifang
 *
 */
public class TestGenericAvroTransformer extends GenericAvroEventTransformer {

	@Override
	public GenericRecord transform(JetstreamEvent event) {
		Record record = new GenericData.Record(this.getSchema());

		record.put("key1", (String) event.get("key1"));
		record.put("key2", (String) event.get("key2"));
		return record;
	}

}
