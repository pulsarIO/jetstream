/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.management;



import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.xmlser.XSerializable;

/**
 * 
 * @author shmurthy@ebay.com (shmurthy@ebay.com)
 * 
 */

@ManagedResource(objectName = "Management", description = "PingServlet")
public class PingServlet extends HttpServlet implements XSerializable {

  private static final long serialVersionUID = 1L;
 
 
  public PingServlet() {
   
  }
  
  
  public void init() {
	  Management.removeBeanOrFolder("Management/LBPingServlet"); 
	  Management.addBean("LBPingServlet", this);
  }
  
  public void destroy() {
	  
  }

 

 

  @SuppressWarnings("unchecked")
  @Override
  public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

	  //
      // Create a DateFormatter object for displaying date information.
      //
	 
      DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
      

     //
      // Get date and time information in milliseconds
      //
      long now = System.currentTimeMillis();

      //
      // Create a calendar object that will convert the date and time value
      // in milliseconds to date. We use the setTimeInMillis() method of the
      // Calendar object.
      //
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(now);
    
	
	String respStr = "status=AVAILABLE&ServeTraffic=true&host" + InetAddress.getLocalHost().getHostName() + "&time=" + formatter.format(calendar.getTime());
    response.setContentType("text/plain");
    PrintWriter writer = response.getWriter();
    writer.append(respStr);
    writer.flush();
    response.setContentLength(respStr.length());
        
  }

  
}