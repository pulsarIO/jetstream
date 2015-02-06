/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ZooKeeperDataWrapper implements Externalizable{
	
	private static final long serialVersionUID = 1L;
	private long timestamp = System.currentTimeMillis()/1000;
	private Object orginalData;
	
	
	public ZooKeeperDataWrapper() {
	
	}
	
	public ZooKeeperDataWrapper(Object originalData){
		this.orginalData = originalData;
	}
	
	public Object getOrginalData() {
		return orginalData;
	}
	public void setOrginalData(Object orginalData) {
		this.orginalData = orginalData;
	}
	public long getTimestamp() {
		return timestamp;
	}
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(timestamp);
		out.writeObject(orginalData);
		
	}
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		timestamp = in.readLong();
		orginalData = in.readObject();
		
	}

}
