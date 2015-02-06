/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

import io.netty.handler.codec.http.HttpResponse;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

public interface ResponseFuture {

    public void operationComplete(HttpResponse response);

    public void setFailure();

    public void setSuccess();

    public void setTimedout();

}
