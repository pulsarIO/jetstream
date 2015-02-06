/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.file;

import java.io.IOException;
import java.util.List;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.event.channel.ChannelAddress;


/* The default delimiter is a comma */
public class DelimitedEventLog extends OutboundRollingFileChannel {
	
	private String delimiter = ",";
	private List<String> keys;

	public DelimitedEventLog(String streamType, String fileName, int backups, String fileSize) throws IOException {
		super(streamType, fileName, backups, fileSize);
	}

	@Override
	public ChannelAddress getAddress() {
		return null;
	}

	@Override
	String createEventLine(JetstreamEvent e) {
		StringBuffer sb  = new StringBuffer();
		boolean isFirst = true;
		for (String key: getKeys()) {
			if (isFirst) {
				isFirst = false;
			} else {
				sb.append(delimiter);
			}
			if (e.get(key) != null) {
				sb.append(e.get(key));
			}
		}
		return sb.toString();
	}
	
	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public List<String> getKeys() {
		return keys;
	}

	public void setKeys(List<String> keys) {
		this.keys = keys;
	}

}
