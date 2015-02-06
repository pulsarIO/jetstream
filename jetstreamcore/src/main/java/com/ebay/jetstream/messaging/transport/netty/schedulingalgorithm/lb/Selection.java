/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.transport.netty.schedulingalgorithm.lb;

/**
 * 
 * 
 * @author shmurthy@ebay.com
 * @version 1.0
 * 
 * This is an interface that must be implemented by classes that implement 
 * selection algorithms
 * 
 * 
 */
public interface Selection {

	/**
	 * @param state
	 */
	public void setSelectionState(Object state);
    
    
	/**
	 * @return
	 */
	public Object getSelectionState();
    
	/**
	 * @return
	 */
	public boolean isBusy();
}
