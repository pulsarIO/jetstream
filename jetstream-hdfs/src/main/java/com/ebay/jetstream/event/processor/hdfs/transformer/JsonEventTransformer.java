/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.transformer;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.EventTransformer;
import com.ebay.jetstream.event.processor.hdfs.util.JsonUtil;

/**
 * @author weifang
 * 
 */
public class JsonEventTransformer implements EventTransformer<String> {

	@Override
	public String transform(JetstreamEvent event) {
		return JsonUtil.mapToJsonString(event);
	}

}
