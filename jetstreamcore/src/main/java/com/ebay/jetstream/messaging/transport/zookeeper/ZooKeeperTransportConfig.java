/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.zookeeper;


import java.util.ArrayList;
import java.util.List;

import com.ebay.jetstream.messaging.config.TransportConfig;
import com.ebay.jetstream.xmlser.XSerializable;
/**
 * 
 * Holds ZooKeeper Specific configuration
 * 
 * @author rmuthupandian
 *
 */
public class ZooKeeperTransportConfig extends TransportConfig implements XSerializable
{
    
    private int cxnWaitInMillis = 4000;
    private int sessionTimeoutInMillis = 300000;
    private int retrycount = 3;
    private int retryWaitTimeInMillis = 100;
    private int cnxnBalanceIntervalInHrs = 6; // make it big, so connection flaps will not occuer often
	private List<ZooKeeperNode> zknodes =  new ArrayList<ZooKeeperNode>();
	private List<String> nettyDiscoveryProtocolTopics = new ArrayList<String>();
	private int requestQueueDepth = 50000;
	private int pingerIntervalInMins = 10;
	private int pingerCnxnLostThreshold= 5;

    public int getPingerCnxnLostThreshold() {
		return pingerCnxnLostThreshold;
	}

	public void setPingerCnxnLostThreshold(int pingerCnxnLostThreshold) {
		this.pingerCnxnLostThreshold = pingerCnxnLostThreshold;
	}

	public int getPingerIntervalInMins() {
		return pingerIntervalInMins;
	}

	public void setPingerIntervalInMins(int pingerIntervalInMins) {
		this.pingerIntervalInMins = pingerIntervalInMins;
	}

	public int getRequestQueueDepth() {
		return requestQueueDepth;
	}

	public void setRequestQueueDepth(int requestQueueDepth) {
		this.requestQueueDepth = requestQueueDepth;
	}

	public List<String> getNettyDiscoveryProtocolTopics() {
		return nettyDiscoveryProtocolTopics;
	}

	public void setNettyDiscoveryProtocolTopics(List<String> nettyDiscoverableTopics) {
		this.nettyDiscoveryProtocolTopics = nettyDiscoverableTopics;
	}

	public int getCnxnBalanceIntervalInHrs() {
		return cnxnBalanceIntervalInHrs;
	}

	public void setCnxnBalanceIntervalInHrs(int cnxnBalanceIntervalInHrs) {
		this.cnxnBalanceIntervalInHrs = cnxnBalanceIntervalInHrs;
	}

	public int getRetrycount() {
		return retrycount;
	}

	public void setRetrycount(int retrycount) {
		this.retrycount = retrycount;
	}

	public int getRetryWaitTimeInMillis() {
		return retryWaitTimeInMillis;
	}

	public void setRetryWaitTimeInMillis(int retryWaitTimeInMillis) {
		this.retryWaitTimeInMillis = retryWaitTimeInMillis;
	}

 	public List<ZooKeeperNode> getZknodes() {
		return zknodes;
	}

	public void setZknodes(List<ZooKeeperNode> zknodes) {
		this.zknodes = zknodes;
	}

	public int getCxnWaitInMillis() {
		return cxnWaitInMillis;
	}

	public void setCxnWaitInMillis(int cxnWaitInMillis) {
		this.cxnWaitInMillis = cxnWaitInMillis;
	}
	
	public int getSessionTimeoutInMillis() {
		return sessionTimeoutInMillis;
	}

	public void setSessionTimeoutInMillis(int sessionTimeoutInMillis) {
		this.sessionTimeoutInMillis = sessionTimeoutInMillis;
	}


    
    @Override
    public boolean requireDNS() {
    	return false;
    }
    
 
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + cxnWaitInMillis;
		result = prime * result + sessionTimeoutInMillis;
		result = prime * result + retryWaitTimeInMillis;
		result = prime * result + retrycount;
		result = prime * result + requestQueueDepth;
		result = prime * result + ((zknodes == null) ? 0 : zknodes.hashCode());
		result = prime * result + cnxnBalanceIntervalInHrs;
		result = prime * result + pingerIntervalInMins;
		result = prime * result + pingerCnxnLostThreshold;
		result = prime * result +  ((nettyDiscoveryProtocolTopics == null) ? 0 : nettyDiscoveryProtocolTopics.hashCode());;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		ZooKeeperTransportConfig other = (ZooKeeperTransportConfig) obj;
		if (cxnWaitInMillis != other.cxnWaitInMillis)
			return false;
		if (sessionTimeoutInMillis != other.sessionTimeoutInMillis)
			return false;
		if (retryWaitTimeInMillis != other.retryWaitTimeInMillis)
			return false;
		if (retrycount != other.retrycount)
			return false;
		if (cnxnBalanceIntervalInHrs != other.cnxnBalanceIntervalInHrs)
			return false;
		if (pingerIntervalInMins != other.pingerIntervalInMins)
			return false;
		if (pingerCnxnLostThreshold != other.pingerCnxnLostThreshold)
			return false;
		if(requestQueueDepth != other.requestQueueDepth)
			return false;
		if (zknodes == null) {
			if (other.zknodes != null)
				return false;
		} else{
			
			if(this.getZknodes().size() != other.getZknodes().size())
	    		return false;
	    	
	    	boolean match = false;
	    	for(ZooKeeperNode zknode : other.getZknodes()){
	    		for(ZooKeeperNode thisnode : this.getZknodes()){
	    			 match = thisnode.equals(zknode);
	    			 if(match)
	    				 break;
	    		}
	    	}
	    	if(!match)
	    		return false;
		}
		
		if (nettyDiscoveryProtocolTopics == null) {
			if (other.nettyDiscoveryProtocolTopics != null)
				return false;
		} else{
			if(this.getNettyDiscoveryProtocolTopics().size() != other.getNettyDiscoveryProtocolTopics().size())
	    		return false;
	    	
			if(!this.getNettyDiscoveryProtocolTopics().isEmpty() && !other.getNettyDiscoveryProtocolTopics().isEmpty()){
		
				boolean match = false;
		    	for(String nettyTopic : other.getNettyDiscoveryProtocolTopics()){
		    		for(String thisnode : this.getNettyDiscoveryProtocolTopics()){
		    			 match = thisnode.equals(nettyTopic);
		    			 if(match)
		    				 break;
		    		}
		    	}
		    	if(!match)
		    		return false;
			}
		}	
		
			
		return true;
	}

	@Override
	public String toString() {
		return "ZooKeeperTransportConfig [cxnWaitInMillis=" + cxnWaitInMillis
				+ ", sessionTimeoutInMillis=" + sessionTimeoutInMillis
				+ ", retrycount=" + retrycount + ", retryWaitTimeInMillis="
				+ retryWaitTimeInMillis + ", cnxnBalanceIntervalInHrs="
				+ cnxnBalanceIntervalInHrs + ", zknodes=" + zknodes
				+ ", nettyDiscoveryProtocolTopics="
				+ nettyDiscoveryProtocolTopics + ", requestQueueDepth="
				+ requestQueueDepth + ", pingerIntervalInMins="
				+ pingerIntervalInMins + ", pingerCnxnLostThreshold="
				+ pingerCnxnLostThreshold + "]";
	}

	
	
	
	
}
