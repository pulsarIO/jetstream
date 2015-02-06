/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.messaging.http;


import io.netty.handler.codec.http.HttpHeaders;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */


public class TestServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.http.inbound");
  private transient HttpMsgListener m_ml;
  
  private final transient ObjectMapper mapper = new ObjectMapper();

  public TestServlet() {
	  
  }
  
  public TestServlet(HttpMsgListener ml) {
    m_ml = ml;
  }

  private String getContent(HttpServletRequest request) {
	InputStreamReader isr = null;
	BufferedReader br = null;
	  
    try {
      ServletInputStream content = request.getInputStream();
      isr = new InputStreamReader(content);
      
      br = new BufferedReader(isr);
      String thisLine;

      StringBuffer sb = new StringBuffer(request.getContentLength());
      while ((thisLine = br.readLine()) != null) {
        sb.append(thisLine);
      }
      return sb.toString();
    }
    catch (IOException e) {
      LOGGER.error( "error unmarshalling JSON context -" + e.getLocalizedMessage());

      return "";
    }
    finally {
    	
    	if (isr != null) {
			try {
				isr.close();
			} catch (IOException e) {
				LOGGER.error( "Failed to close stream -" + e.getLocalizedMessage());
			}
    	}
    	
    	if (br != null)
			try {
				br.close();
			} catch (IOException e) {
				LOGGER.error( "Failed to close stream -" + e.getLocalizedMessage());
			}
    }
  }

  
  @SuppressWarnings("unchecked")
  @Override
  public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

	HashMap event = null;
   
	try {
		
		System.out.println("request header"+request.getHeader(HttpHeaders.Names.ACCEPT_ENCODING));
		//HttpHeaders.getHeader((HttpMessage) request., HttpHeaders.Names.ACCEPT_ENCODING)
      event = mapper.readValue(getContent(request), HashMap.class);

    }
    catch (Throwable t) {
      LOGGER.error( "error unmarshalling JSON context -" + t.getLocalizedMessage());
      return;
    }

    m_ml.onMessage(event);
    
    response.setContentLength(0);

  }
  
}