/**
 * 
 */
package com.ebay.jetstream.event.processor.hdfs.progress;

import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.common.ShutDownable;
import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.event.processor.hdfs.HdfsClient;
import com.ebay.jetstream.event.processor.hdfs.PartitionKey;
import com.ebay.jetstream.event.processor.hdfs.PartitionProgressController;
import com.ebay.jetstream.event.processor.hdfs.ProgressController;
import com.ebay.jetstream.event.processor.hdfs.ProgressStore;
import com.ebay.jetstream.event.processor.hdfs.util.DateUtil;
import com.ebay.jetstream.event.processor.hdfs.util.JsonUtil;

/**
 * @author weifang
 * 
 */
public class TimeBasedProgressController extends AbstractNamedBean implements
		InitializingBean, ShutDownable, ProgressController {
	private static final Logger LOGGER = Logger
			.getLogger(TimeBasedProgressController.class.getName());

	// injected
	protected TimeBasedProgressControllerConfig config;
	protected ProgressStore store;
	protected HdfsClient hdfs;

	// internal
	protected TimeBasedProgressHelper helper;
	protected String outputFolder;

	@Override
	public void setOutputFolder(String folder) {
		this.outputFolder = folder;
	}

	public void setConfig(TimeBasedProgressControllerConfig config) {
		this.config = config;
	}

	public void setStore(ProgressStore store) {
		this.store = store;
	}

	public void setHdfs(HdfsClient hdfs) {
		this.hdfs = hdfs;
	}

	@Override
	public int getPendingEvents() {
		return 0;
	}

	@Override
	public void shutDown() {
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		helper = new TimeBasedProgressHelper(config, store);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ebay.jetstream.event.processor.hdfs.ProgressControllerFactory#
	 * createProgressController()
	 */
	@Override
	public PartitionProgressController createPartitionProgressController(
			PartitionKey key) {
		return new TimeBasedPartitionProgressController(config, helper, key);
	}

	@Override
	public List<String> getSuccessCheckFolders() {
		List<String> ret = new LinkedList<String>();
		long minFolderTs = helper.getMinWorking();
		if (minFolderTs == 0) {
			LOGGER.log(Level.INFO,
					"No working log util now. Wait to check success in next round.");
			return ret;
		}
		long folderTs = minFolderTs;
		int checkRange = config.getSuccessCheckCount();
		for (int i = 0; i < checkRange; i++) {
			folderTs = folderTs - config.getFolderIntervalInMs();
			ret.add(DateUtil.formatDate(folderTs, config.getFolderPathFormat()));
		}

		return ret;
	}

	@Override
	public boolean isSuccess(String folder) {
		try {
			if (!hdfs.exist(outputFolder + "/" + folder)) {
				return false;
			} else {
				return true;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String getSuccessPath(String folder) {
		return outputFolder + "/" + folder + "/" + config.getSuccessFileName();
	}

	@Override
	public Map<String, Map<String, Object>> getFileStats(String folder) {
		try {
			if (hdfs.exist(getSuccessPath(folder))) {
				// already created success file, just return null to let the
				// caller skip this folder
				return null;
			}
			long ts = helper.getTimeSlot(folder);
			return helper.readFileStats(ts);
		} catch (ParseException e) {
			LOGGER.log(Level.SEVERE, "Unrecognized folder " + folder, e);
			return null;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.toString(), e);
			return null;
		}
	}

	@Override
	public void markSuccess(String folder,
			Map<String, Map<String, Object>> fileStats,
			Map<String, Object> aggStats) {
		OutputStream os = null;
		try {
			long ts = helper.getTimeSlot(folder);
			Map<String, Object> ret = new LinkedHashMap<String, Object>();
			ret.put("overview", aggStats);
			ret.put("files", fileStats);
			os = hdfs.createFile(getSuccessPath(folder), true);
			JsonUtil.mapToJsonStream(ret, os);

			helper.deleteFileStats(ts);
		} catch (ParseException e) {
			throw new RuntimeException("Unrecognized folder " + folder, e);
		} catch (IOException e) {
			throw new RuntimeException(
					"Failed to output success file for folder " + folder, e);
		} finally {
			if (os != null) {
				try {
					os.close();
				} catch (IOException e) {
					LOGGER.log(Level.SEVERE, e.toString(), e);
				}
			}
		}
	}

}
