/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import java.util.List;
import java.util.Map;

/**
 * @author weifang
 * 
 */
public interface ProgressStore {
	boolean exists(String path);

	void delete(String path);

	List<String> getChildren(String path);

	byte[] readBytes(String path);

	Map<String, Object> readMap(String path);

	void writeBytes(String path, byte[] bytes);

	void writeMap(String path, Map<String, Object> map);

	boolean available();
}
