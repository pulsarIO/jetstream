/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.event.channel.messaging.http.inbound;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.event.JetstreamEvent;
import com.ebay.jetstream.xmlser.Hidden;
import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={"NN_NAKED_NOTIFY"})

@ManagedResource(objectName = "Event/Channel", description = "InboundRTBDServlet")
public class InboundRTBDEventServlet extends HttpServlet implements XSerializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.messaging.http.inbound");

  private transient InboundRESTChannel m_inboundChannel; 

  private final transient ObjectMapper mapper = new ObjectMapper(); // NOPMD

  public InboundRTBDEventServlet() {}
  
  public InboundRTBDEventServlet(InboundRESTChannel inboundChannel) {
    m_inboundChannel = inboundChannel;
  }

  
  protected String getContent(HttpServletRequest request) {
	  
	InputStreamReader isr = null;
    BufferedReader br = null;
    
    try {
      ServletInputStream content = request.getInputStream();
      if (content == null) return "";
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
				//ignore
			}
    	}
    	
    	if (br != null) {
    		try {
				br.close();
			} catch (IOException e) {
				//ignore 
			}
    	}
    }
  }

  /**
   * @return the inboundChannel
   */
  
  @Hidden
  public InboundRESTChannel getInboundChannel() {
    return m_inboundChannel;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    ArrayList<Object> eventList = null;

    try {

      eventList = mapper.readValue(getContent(request), ArrayList.class);

    }
    catch (Throwable t) {
      LOGGER.error( "error unmarshalling JSON context -" + t.getLocalizedMessage());
      return;
    }

    JetstreamEvent event = null;

    Iterator<Object> itr = eventList.iterator();

    while (itr.hasNext()) {

      Map<String, Object> map = (Map<String, Object>) itr.next();

      event = new JetstreamEvent(map); // obj would be LinkedHashMap

      if (event != null) {

        m_inboundChannel.onMessage(event);

      }
    }
    
    response.setContentLength(0);

  }

  /**
   * @param inboundChannel
   *          the inboundChannel to set
   */
  public void setInboundChannel(InboundRESTChannel inboundChannel) {
    m_inboundChannel = inboundChannel;
  }
}