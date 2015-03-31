/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.jetstream.application.JetstreamApplication;
import com.ebay.jetstream.event.BatchSource;
import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.processor.hdfs.util.DateUtil;
import com.ebay.jetstream.event.processor.hdfs.util.IOUtil;
import com.ebay.jetstream.event.processor.hdfs.util.JsonUtil;

/**
 * @author weifang
 * 
 */
public class CommonTest extends JetstreamTestApp {
	private static final String ROOT_FOLDER = "/tmp/pulsar/common_test";
	private static final String TOPIC = "topic1";
	private static final String TIMESTAMP_KEY = "test_timestamp";
	private static final String START_TS = "20150101_01:01:00";

	private static ZookeeperTestServer zkServer;

	@BeforeClass
	public static void setup() throws Exception {
		IOUtil.delTree(ROOT_FOLDER);
		zkServer = new ZookeeperTestServer(30000, 21819, 100);
		zkServer.startup();
		JetstreamTestApp.startApp("common-test.xml", 9998);
	}

	@AfterClass
	public static void cleanup() throws Exception {
		JetstreamTestApp.stopApp();
		zkServer.shutdown();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void test1() throws Exception {
		long start = DateUtil.parseDateFromLog(START_TS).getTime();
		int batchSize = 50;
		HdfsBatchProcessor processor = JetstreamApplication.getConfiguration()
				.getBean(HdfsBatchProcessor.class);
		for (int i = 0; i < 10; i++) {
			BatchSource src = new BatchSource(TOPIC, 0, i * batchSize);
			List<JetstreamEvent> events = fakeEvents(batchSize, start + i
					* 5000);
			processor.onNextBatch(src, events);
		}
		Thread.sleep(5000);
		File outDir = new File("/tmp/pulsar/common_test/out");
		File[] subDirs = outDir.listFiles();
		assertEquals(1, subDirs.length);
		assertEquals("20150101", subDirs[0].getName());
		File[] tsDirs = subDirs[0].listFiles();
		assertEquals(4, tsDirs.length);
		File firstTsDir = new File(outDir, "20150101/01_01_00");
		File successFile = new File(firstTsDir, "_SUCCESS");
		assertEquals(true, successFile.exists());
		FileInputStream fis = new FileInputStream(successFile);
		Map<String, Object> json = JsonUtil.jsonStreamToMap(fis);
		assertEquals(1, json.get("fileCount"));
		Map<String, Object> files = (Map<String, Object>) json.get("files");
		assertEquals(1, files.size());

		File tsDir2 = new File(outDir, "20150101/01_01_10/type1");
		File dataFile = new File(tsDir2, "topic1-0-150-249");
		assertEquals(true, dataFile.exists());
		BufferedReader reader = new BufferedReader(new FileReader(dataFile));
		Map<String, Object> lineJson = JsonUtil.jsonStringToMap(reader
				.readLine());
		assertEquals("value1", lineJson.get("key1"));
		reader.close();
	}

	private List<JetstreamEvent> fakeEvents(int count, long timeSlot) {
		List<JetstreamEvent> fakes = new ArrayList<JetstreamEvent>();
		for (int i = 0; i < count; i++) {
			JetstreamEvent jsEvent = new JetstreamEvent();
			if (i % 2 == 0) {
				jsEvent.setEventType("type1");
			} else {
				jsEvent.setEventType("type2");
			}
			jsEvent.put(TIMESTAMP_KEY, timeSlot + i);
			jsEvent.put("key1", "value1");
			jsEvent.put("key2", "value2" + i);
			fakes.add(jsEvent);
		}
		return fakes;
	}
}
