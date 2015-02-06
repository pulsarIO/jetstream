/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 * 
 */
package com.ebay.jetstream.event.processor.kafka;

import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * @author weifang
 * 
 */
public class SimpleKafkaProcessorConfig extends AbstractNamedBean implements
		XSerializable {
	public static final String AUTO_ADVANCE_EVERYBATCH = "every-batch";
	public static final String AUTO_ADVANCE_PERIODIC = "periodic";

	private String autoCommitMode = AUTO_ADVANCE_PERIODIC;
	private boolean noDuplication = false;

	private long autoCommitInterval = 30 * 1000;

	private int maxReadRate = -1;

	public String getAutoAdvanceMode() {
		return autoCommitMode;
	}

	public void setAutoAdvanceMode(String autoAdvanceMode) {
		this.autoCommitMode = autoAdvanceMode;
	}

	public long getAutoAdvanceInterval() {
		return autoCommitInterval;
	}

	public void setAutoAdvanceInterval(long autoAdvanceInterval) {
		this.autoCommitInterval = autoAdvanceInterval;
	}

	public int getMaxReadRate() {
		return maxReadRate;
	}

	public void setMaxReadRate(int maxReadRate) {
		this.maxReadRate = maxReadRate;
	}

	public boolean isNoDuplication() {
		return noDuplication;
	}

	public void setNoDuplication(boolean noDuplication) {
		this.noDuplication = noDuplication;
	}

}
