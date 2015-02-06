/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.management;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.util.CommonUtils;

@ManagedResource(objectName = "Meta/Management", description = "http object management interface")
public class ManagementServlet extends HttpServlet {
  private static final Logger LOGGER = LoggerFactory.getLogger(ManagementServlet.class.getName());
  private static final long serialVersionUID = 1L;
  private static int MAX_URL_LENGTH = 250;
  private Validator validator ;

static {
    Management.registerResourceFormatter("xml", XmlResourceFormatter.class);
    Management.registerResourceFormatter("spring", SpringResourceFormatter.class);
    Management.registerResourceFormatter("html", HtmlResourceFormatter.class);
    Management.registerResourceFormatter("json", JsonResourceFormatter.class);
    Management.registerResourceFormatter("help", HelpFormatter.class);
  }

  

  public ManagementServlet() {
    Management.addBean(toString(), this);
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    if (!checkAuthorized(request, response, false))
      return;
    response.setCharacterEncoding("UTF-8");
    Map<String, String[]> parameters = getParameterMap(request);
    boolean isHelp = parameters.remove("help") != null;
    if (isHelp && parameters.size() > 0) {
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "help cannot be combined with other parameters");
      return;
    }
    String format = getParameter(parameters, AbstractResourceFormatter.BEAN_FORMAT_PARAM);
    if (isHelp || CommonUtils.isEmptyTrimmed(format)) {
      format = "help";
    }
    
    String beanLocation[] = getBeanLocation(request);
    
    for(String bloc : beanLocation){
    	if(!validate(bloc)){
        	sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid Request URL");
        }
    }
    
    try {
      response.setHeader("Cache-Control", "no-cache");
      BeanController bc = new BeanController(beanLocation[0], beanLocation[1]);
      bc.setRequestedFields(request.getParameterValues("field"));
      parameters.remove("field");
      if (parameters.size() > 0) {
        if (!checkAuthorized(request, response, true))
          return;
        bc.process(parameters);
      }
      else {
        bc.setFormat(format);
        response.setContentType(bc.getContentType());
        bc.write(response.getWriter());
      }
    }
    catch (Throwable t) {
      sendException(response, "failed for " + beanLocation[1], t);
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    if (!checkAuthorized(request, response, true))
      return;
    // Reading post content must happen early
    String content = CommonUtils.getStreamAsString(request.getInputStream(), "\n");
    Map<String, String[]> parameters = getParameterMap(request);
    String format = getParameter(parameters, AbstractResourceFormatter.BEAN_FORMAT_PARAM);
    String form[] = parameters.remove("form");
    String actions[] = parameters.remove("action");
    String properties[] = parameters.remove("property");
    if (format == null) {
      format = "spring";
    }
    if (form != null) {
      content = URLDecoder.decode(content, "UTF-8");
      if (form.length != 1 && CommonUtils.isEmptyTrimmed(form[0]) || properties != null) {
        sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Cannot specify both form and property");
        return;
      }
      int p = content.indexOf("=");
      properties = new String[] { content.substring(0, p) };
      content = content.substring(p + 1);
    }
    if (actions != null && properties != null) {
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Cannot specify both action and property");
      return;
    }
    if (properties != null && CommonUtils.isEmptyTrimmed(content)) {
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Must send content to set property");
      return;
    }

    if (actions != null)
      for (String action : actions)
        parameters.put(action, null);

    if (properties != null)
      for (String property : properties)
        parameters.put(property, new String[] { format, content });
    
    if(!validate(request.getPathInfo()) || !validate(request.getRequestURL().toString())){
    	sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid Request URL");
    }

    String beanLocation[] = getBeanLocation(request);
    
    for(String bloc : beanLocation){
    	if(!validate(bloc)){
        	sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid Request URL");
        }
    }
    try {
      response.setHeader("Cache-Control", "no-cache");
      BeanController bc = new BeanController(beanLocation[0], beanLocation[1]);
      bc.process(parameters);
      response.setContentType(bc.getContentType());
    }
    catch (Throwable t) {
      sendException(response, "POST failed", t);
    }
  }

  private boolean checkAuthorized(HttpServletRequest request, HttpServletResponse response, boolean forWrite) {
    ManagementNetworkSecurity mns = ManagementNetworkSecurity.getInstance();
    boolean authorized = mns == null;
    try {
      if (!authorized)
        authorized = mns.isAuthorized(InetAddress.getByName(request.getRemoteAddr()), forWrite);
    }
    catch (UnknownHostException e) {
    }
    if (!authorized)
      sendError(response, HttpServletResponse.SC_UNAUTHORIZED, request.getRemoteAddr() + " access not allowed");
    return authorized;
  }

  private String[] getBeanLocation(HttpServletRequest request) {
    String result[] = new String[2];
    String	base = request.getPathInfo();
    result[1] = base == null ? "" : base.substring(1);
    int suffixToCut = result[1].length();
    String url = request.getRequestURL().toString();
    result[0] = url.substring(0, url.length() - suffixToCut);
    return result;
  }
  
  private boolean validate(String reqUrl){
	  if(validator != null)
		  return validator.validate(reqUrl);
	  else
		  return true;
  }

  private String getParameter(Map<String, String[]> map, String key) {
    String values[] = map.remove(key);
    return values == null || values.length == 0 ? null : values[0];
  }

  @SuppressWarnings("unchecked")
  private Map<String, String[]> getParameterMap(HttpServletRequest request) {
    return new HashMap<String, String[]>(request.getParameterMap());
  }

  private void sendError(HttpServletResponse response, int statusCode, String message) {
    try {
      response.sendError(statusCode, message);
    }
    catch (Throwable t) {
      throw CommonUtils.runtimeException(t);
    }
  }

  private void sendException(HttpServletResponse response, String message, Throwable cause) {
    String errorText = message + ": " + cause + ".  " + CommonUtils.redirectPrintStackTraceToString(cause);
    if (cause instanceof IOException)
      LOGGER.warn("IOException: " + errorText);
    else if (cause instanceof IllegalArgumentException) {
      LOGGER.warn("Bad client request: " + errorText);
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, errorText);
    }
    else {
      LOGGER.error("INTERNAL SERVER ERROR: " + errorText);
      sendError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, errorText);
    }
  }
}
