/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.http.netty.client;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/*
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
*/

public class ServerHttpResponseWrapper implements HttpResponse {

  DefaultHttpResponse m_response;

  ServerHttpResponseWrapper(DefaultHttpResponse response) {
    m_response = response;
  }

@Override
public HttpVersion getProtocolVersion() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public HttpHeaders headers() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public DecoderResult getDecoderResult() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setDecoderResult(DecoderResult result) {
	// TODO Auto-generated method stub
	
}

@Override
public HttpResponseStatus getStatus() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public HttpResponse setStatus(HttpResponseStatus status) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public HttpResponse setProtocolVersion(HttpVersion version) {
	// TODO Auto-generated method stub
	return null;
}

  /*
  @Override
  public void addHeader(String name, Object value) {

  }

  @Override
  public void clearHeaders() {
    // TODO Auto-generated method stub

  }
   
  
  @Override
  public boolean containsHeader(String name) {
    return m_response.containsHeader(name);
  }

  @Override
  public ChannelBuffer getContent() {
    return m_response.getContent();
  }

  @Override
  public long getContentLength() {
    return Long.parseLong(m_response.getHeader(HttpHeaders.Names.CONTENT_LENGTH));
  }

  @Override
  public long getContentLength(long defaultValue) {
    return HttpHeaders.getContentLength(m_response, defaultValue);

  }

  
  
  @Override
  public String getHeader(String name) {
    return m_response.getHeader(name);
  }

  @Override
  public Set<String> getHeaderNames() {
    return m_response.getHeaderNames();
  }

  @Override
  public List<Entry<String, String>> getHeaders() {
    return m_response.getHeaders();
  }

  @Override
  public List<String> getHeaders(String name) {
    return m_response.getHeaders(name);
  }

  @Override
  public HttpVersion getProtocolVersion() {
    return m_response.getProtocolVersion();
  }

  @Override
  public HttpResponseStatus getStatus() {
    return m_response.getStatus();
  }

  @Override
  public boolean isChunked() {
    return m_response.isChunked();
  }
  
 
  
  @Override
  public boolean isKeepAlive() {
    return m_response.isKeepAlive();
  }

  @Override
  public void removeHeader(String name) {

  }

  @Override
  public void setChunked(boolean chunked) {

  }

  @Override
  public void setContent(ChannelBuffer content) {

  }

  @Override
  public void setHeader(String name, Iterable<?> values) {

  }

  @Override
  public void setHeader(String name, Object value) {

  }

  @Override
  public void setProtocolVersion(HttpVersion version) {

  }

  @Override
  public void setStatus(HttpResponseStatus status) {

  }

@Override
public HttpHeaders headers() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public DecoderResult getDecoderResult() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setDecoderResult(DecoderResult result) {
	// TODO Auto-generated method stub
	
}

@Override
public HttpResponse setStatus(HttpResponseStatus status) {
	// TODO Auto-generated method stub
	return null;
}

@Override
public HttpResponse setProtocolVersion(HttpVersion version) {
	// TODO Auto-generated method stub
	return null;
}

*/
  

}
